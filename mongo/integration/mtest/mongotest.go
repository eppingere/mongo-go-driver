package mtest

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"math"
	"strconv"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
)

var (
	// Background is a no-op context.
	Background = context.Background()
	// MajorityWc is the majority write concern.
	MajorityWc = writeconcern.New(writeconcern.WMajority())
	// PrimaryRp is the primary read preference.
	PrimaryRp = readpref.Primary()
	// MajorityRc is the majority read concern.
	MajorityRc = readconcern.Majority()
)

const (
	namespaceExistsErrCode int32 = 48
)

// FailPoint is a representation of a server fail point.
// See https://github.com/mongodb/specifications/tree/master/source/transactions/tests#server-fail-point
// for more information regarding fail points.
type FailPoint struct {
	ConfigureFailPoint string `json:"configureFailPoint" bson:"configureFailPoint"`
	// Mode should be a string, FailPointMode, or map[string]interface{}
	Mode interface{}   `json:"mode" bson:"mode"`
	Data FailPointData `json:"data" bson:"data"`
}

// FailPointMode is a representation of the Failpoint.Mode field.
type FailPointMode struct {
	Times int32 `json:"times" bson:"times"`
	Skip  int32 `json:"skip" bson:"skip"`
}

// FailPointData is a representation of the FailPoint.Data field.
type FailPointData struct {
	FailCommands                  []string `json:"failCommands" bson:"failCommands,omitempty"`
	CloseConnection               bool     `json:"closeConnection" bson:"closeConnection,omitempty"`
	ErrorCode                     int32    `json:"errorCode" bson:"errorCode,omitempty"`
	FailBeforeCommitExceptionCode int32    `json:"failBeforeCommitExceptionCode" bson:"failBeforeCommitExceptionCode,omitempty"`
	WriteConcernError             *struct {
		Code   int32  `json:"code" bson:"code"`
		Name   string `json:"codeName" bson:"codeName"`
		Errmsg string `json:"errmsg" bson:"errmsg"`
	} `json:"writeConcernError" bson:"writeConcernError,omitempty"`
}

// T is a wrapper around testing.T.
type T struct {
	*testing.T

	// members for only this T instance
	createClient *bool
	constraints  []RunOnBlock
	mockDeployment *mockDeployment // nil if the test is not being run against a mock
	mockResponses []bson.D
	createdColls []*mongo.Collection // collections created in this test
	dbName, collName string
	failPointNames []string

	// options copied to sub-tests
	clientType   ClientType
	clientOpts   *options.ClientOptions
	collOpts     *options.CollectionOptions

	baseOpts *Options // used to create subtests

	// command monitoring channels
	started []*event.CommandStartedEvent
	succeeded []*event.CommandSucceededEvent
	failed []*event.CommandFailedEvent

	Client *mongo.Client
	Coll   *mongo.Collection
}

// New creates a new T instance with the given options. If the current environment does not satisfy constraints
// specified in the options, the test will be skipped automatically.
func New(wrapped *testing.T, opts ...*Options) *T {
	t := &T{
		T: wrapped,
	}
	for _, opt := range opts {
		for _, optFunc := range opt.optFuncs {
			optFunc(t)
		}
	}
	shouldSkip, err := t.shouldSkip()
	if err != nil {
		t.Fatalf("error checking constraints: %v", err)
	}
	if shouldSkip {
		t.Skip("no matching environmental constraint found")
	}

	// create a set of base options for sub-tests
	t.baseOpts = NewOptions().ClientOptions(t.clientOpts).CollectionOptions(t.collOpts).ClientType(t.clientType)
	if t.collName == "" {
		t.collName = t.Name()
	}
	if t.dbName == "" {
		t.dbName = TestDb
	}
	t.collName = sanitizeCollectionName(t.dbName, t.collName)
	return t
}

func sanitizeCollectionName(kind string, name string) string {
	// Collections can't have "$" in their names, so we substitute it with "%".
	name = strings.Replace(name, "$", "%", -1)

	// Namespaces can only have 120 bytes max.
	if len(kind+"."+name) >= 119 {
		name = name[:119-len(kind+".")]
	}

	return name
}

func (t *T) createClientAndCollection() {
	clientOpts := t.clientOpts
	if clientOpts == nil {
		// default opts
		clientOpts = options.Client().SetWriteConcern(MajorityWc).SetReadPreference(PrimaryRp).SetReadConcern(MajorityRc)
	}
	// command monitor
	clientOpts.SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, cse *event.CommandStartedEvent) {
			t.started = append(t.started, cse)
		},
		Succeeded: func(_ context.Context, cse *event.CommandSucceededEvent) {
			t.succeeded = append(t.succeeded, cse)
		},
		Failed: func(_ context.Context, cfe *event.CommandFailedEvent) {
			t.failed = append(t.failed, cfe)
		},
	})

	var err error
	switch t.clientType {
	case Default:
		clientOpts.ApplyURI(testContext.connString.Original)
		t.Client, err = mongo.NewClient(clientOpts)
	case Pinned:
		// pin to first mongos
		clientOpts.ApplyURI(testContext.connString.Original).SetHosts([]string{testContext.connString.Hosts[0]})
		t.Client, err = mongo.NewClient(clientOpts)
	case Mock:
		t.mockDeployment = newMockDeployment()
		t.Client, err = mongo.NewClientFromDeployment(t.mockDeployment, clientOpts)
	}
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}
	if err := t.Client.Connect(Background); err != nil {
		t.Fatalf("error connecting client: %v", err)
	}

	t.Coll = t.Client.Database(t.dbName).Collection(t.collName, t.collOpts)
	t.createdColls = t.createdColls[:0]
	t.createdColls = append(t.createdColls, t.Coll)
}

// Run creates a new T instance for a sub-test and runs the given callback. It also creates a new collection using the
// given name which is available to the callback through the T.Coll variable and is dropped after the callback
// returns.
func (t *T) Run(name string, callback func(*T)) {
	t.RunOpts(name, NewOptions(), callback)
}

// RunOpts creates a new T instance for a sub-test with the given options. If the current environment does not satisfy
// constraints specified in the options, the new sub-test will be skipped automatically. If the test is not skipped,
// the callback will be run with the new T instance. RunOpts creates a new collection with the given name which is
// available to the callback through the T.Coll variable and is dropped after the callback returns.
func (t *T) RunOpts(name string, opts *Options, callback func(*T)) {
	t.T.Run(name, func(wrapped *testing.T) {
		sub := New(wrapped, t.baseOpts, opts)

		// defer dropping all collections only if the sub-test is not running against a mock deployment
		if sub.clientType != Mock {
			defer func() {
				if sub.Client == nil {
					return
				}

				sub.ClearCollections()
				sub.ClearFailPoints()
				_ = sub.Client.Disconnect(Background)
			}()
		}

		if sub.createClient == nil || *sub.createClient {
			sub.createClientAndCollection()
		}

		// add any mock responses for this test
		if sub.clientType == Mock && len(sub.mockResponses) > 0 {
			sub.AddMockResponses(sub.mockResponses...)
		}

		callback(sub)
	})
}

// AddMockResponses adds responses to be returned by the mock deployment. This should only be used if T is being run
// against a mock deployment.
func (t *T) AddMockResponses(responses ...bson.D) {
	t.mockDeployment.addResponses(responses...)
}

// ClearMockResponses clears all responses in the mock deployment.
func (t *T) ClearMockResponses() {
	t.mockDeployment.clearResponses()
}

// GetStartedEvent returns the most recent CommandStartedEvent, or nil if one is not present.
// This can only be called once per event.
func (t *T) GetStartedEvent() *event.CommandStartedEvent {
	if len(t.started) == 0 {
		return nil
	}
	e := t.started[0]
	t.started = t.started[1:]
	return e
}

// GetSucceededEvent returns the most recent CommandSucceededEvent, or nil if one is not present.
// This can only be called once per event.
func (t *T) GetSucceededEvent() *event.CommandSucceededEvent {
	if len(t.succeeded) == 0 {
		return nil
	}
	e := t.succeeded[0]
	t.succeeded = t.succeeded[1:]
	return e
}

// GetFailedEvent returns the most recent CommandFailedEvent, or nil if one is not present.
// This can only be called once per event.
func (t *T) GetFailedEvent() *event.CommandFailedEvent {
	if len(t.failed) == 0 {
		return nil
	}
	e := t.failed[0]
	t.failed = t.failed[1:]
	return e
}

// GetAllStartedEvents returns a slice of all CommandStartedEvent instances for this test. This can be called multiple
// times.
func (t *T) GetAllStartedEvents() []*event.CommandStartedEvent {
	return t.started
}

// GetAllSucceededEvents returns a slice of all CommandSucceededEvent instances for this test. This can be called multiple
// times.
func (t *T) GetAllSucceededEvents() []*event.CommandSucceededEvent {
	return t.succeeded
}

// GetAllFailedEvents returns a slice of all CommandFailedEvent instances for this test. This can be called multiple
// times.
func (t *T) GetAllFailedEvents() []*event.CommandFailedEvent {
	return t.failed
}

// ClearEvents clears the existing command monitoring events.
func (t *T) ClearEvents() {
	t.started = t.started[:0]
	t.succeeded = t.succeeded[:0]
	t.failed = t.failed[:0]
}

// ResetClient resets the existing client with the given options. If opts is nil, the existing options will be used.
// If t.Coll is not-nil, it will be reset to use the new client. Should only be called if the existing client is
// not nil. This will Disconnect the existing client but will not drop existing collections. To do so, ClearCollections
// must be called before calling ResetClient.
func (t *T) ResetClient(opts *options.ClientOptions) {
	if opts != nil {
		t.clientOpts = opts
	}

	_ = t.Client.Disconnect(Background)
	t.createClientAndCollection()
}

// CreateCollection creates a new collection with the given options. The collection will be dropped after the test
// finishes running. If createOnServer is true, the function ensures that the collection has been created server-side
// by running the create command. The create command will appear in command monitoring channels.
func (t *T) CreateCollection(name string, createOnServer bool, opts ...*options.CollectionOptions) *mongo.Collection {
	if createOnServer && t.clientType != Mock {
		cmd := bson.D{{"create", name}}
		if err := t.Client.Database(t.dbName).RunCommand(Background, cmd).Err(); err != nil {
			// ignore NamespaceExists errors for idempotency

			cmdErr, ok := err.(mongo.CommandError)
			if !ok || cmdErr.Code != namespaceExistsErrCode {
				t.Fatalf("error creating collection on server: %v", err)
			}
		}
	}

	coll := t.Client.Database(t.dbName).Collection(name, opts...)
	t.createdColls = append(t.createdColls, coll)
	return coll
}

// ClearCollections drops all collections previously created by this test.
func (t *T) ClearCollections() {
	for _, coll := range t.createdColls {
		_ = coll.Drop(Background)
	}
	t.createdColls = t.createdColls[:0]
}

// SetFailPoint sets a fail point for the client associated with T. Commands to create the failpoint will appear
// in command monitoring channels. The fail point will automatically be disabled after this test has run.
func (t *T) SetFailPoint(fp FailPoint) {
	// ensure mode fields are int32
	if modeMap, ok := fp.Mode.(map[string]interface{}); ok {
		var key string
		var err error

		if times, ok := modeMap["times"]; ok {
			key = "times"
			modeMap["times"], err = t.interfaceToInt32(times)
		}
		if skip, ok := modeMap["skip"]; ok {
			key = "skip"
			modeMap["skip"], err = t.interfaceToInt32(skip)
		}

		if err != nil {
			t.Fatalf("error converting %s to int32: %v", key, err)
		}
	}

	admin := t.Client.Database("admin")
	if err := admin.RunCommand(Background, fp).Err(); err != nil {
		t.Fatalf("error creating fail point on server: %v", err)
	}
	t.failPointNames = append(t.failPointNames, fp.ConfigureFailPoint)
}

// ClearFailPoints disables all previously set failpoints for this test.
func (t *T) ClearFailPoints() {
	db := t.Client.Database("admin")
	for _, fp := range t.failPointNames {
		cmd := bson.D{
			{"configureFailPoint", fp},
			{"mode", "off"},
		}
		err := db.RunCommand(Background, cmd).Err()
		if err != nil {
			t.Fatalf("error clearing fail point %s: %v", fp, err)
		}
	}
	t.failPointNames = t.failPointNames[:0]
}

// AuthEnabled returns whether or not this test is running in an environment with auth.
func (t *T) AuthEnabled() bool {
	return testContext.authEnabled
}

// TopologyKind returns the topology kind of the environment
func (t *T) TopologyKind() TopologyKind {
	return testContext.topoKind
}

// ConnString returns the connection string used to create the client for this test.
func (t *T) ConnString() string {
	return testContext.connString.Original
}

// CloneCollection modifies the default collection for this test to match the given options.
func (t *T) CloneCollection(opts *options.CollectionOptions) {
	var err error
	t.Coll, err = t.Coll.Clone(opts)
	assert.Nil(t, err, "error cloning collection: %v", err)
}

// compareVersions compares two version number strings (i.e. positive integers separated by
// periods). Comparisons are done to the lesser precision of the two versions. For example, 3.2 is
// considered equal to 3.2.11, whereas 3.2.0 is considered less than 3.2.11.
//
// Returns a positive int if version1 is greater than version2, a negative int if version1 is less
// than version2, and 0 if version1 is equal to version2.
func (t *T) compareVersions(v1 string, v2 string) int {
	n1 := strings.Split(v1, ".")
	n2 := strings.Split(v2, ".")

	for i := 0; i < int(math.Min(float64(len(n1)), float64(len(n2)))); i++ {
		i1, err := strconv.Atoi(n1[i])
		if err != nil {
			return 1
		}

		i2, err := strconv.Atoi(n2[i])
		if err != nil {
			return -1
		}

		difference := i1 - i2
		if difference != 0 {
			return difference
		}
	}

	return 0
}

func (t *T) matchesConstraint(rob RunOnBlock) (bool, error) {
	if rob.MinServerVersion != "" && t.compareVersions(testContext.serverVersion, rob.MinServerVersion) < 0 {
		return false, nil
	}
	if rob.MaxServerVersion != "" && t.compareVersions(testContext.serverVersion, rob.MaxServerVersion) > 0 {
		return false, nil
	}
	if rob.MinWireVersion != 0 || rob.MaxWireVersion != 0 {
		srvr, err := testContext.topo.SelectServer(Background, description.ReadPrefSelector(readpref.Primary()))
		if err != nil {
			return false, err
		}
		conn, err := srvr.Connection(Background)
		if err != nil {
			return false, err
		}
		wv := conn.Description().WireVersion.Max

		if rob.MinWireVersion != 0 && wv < rob.MinWireVersion {
			return false, nil
		}
		if rob.MaxWireVersion != 0 && wv > rob.MaxWireVersion {
			return false, nil
		}
	}

	if len(rob.Topology) == 0 {
		return true, nil
	}
	for _, topoKind := range rob.Topology {
		if topoKind == testContext.topoKind {
			return true, nil
		}
	}
	return false, nil
}

func (t *T) shouldSkip() (bool, error) {
	// The test can be executed if there are no constraints or at least one constraint matches the current test setup.
	if len(t.constraints) == 0 {
		return false, nil
	}

	for _, constraint := range t.constraints {
		matches, err := t.matchesConstraint(constraint)
		if err != nil {
			return false, err
		}
		if matches {
			return false, nil
		}
	}
	// no matching constraints found
	return true, nil
}

func (t *T) interfaceToInt32(i interface{}) (int32, error) {
	switch conv := i.(type) {
	case int:
		return int32(conv), nil
	case int32:
		return conv, nil
	case int64:
		return int32(conv), nil
	case float64:
		return int32(conv), nil
	}

	return 0, fmt.Errorf("type %T cannot be converted to int32", i)
}
