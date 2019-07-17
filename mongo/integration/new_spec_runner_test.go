package integration

import (
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"io/ioutil"
	"path"
	"testing"
)

type testFile struct {
	RunOn          []mtest.RunOnBlock `json:"runOn"`
	DatabaseName   string              `json:"database_name"`
	CollectionName string              `json:"collection_name"`
	Data           json.RawMessage    `json:"data"`
	Tests          []testCase         `json:"tests"`
}

type testCase struct {
	Description         string                 `json:"description"`
	SkipReason          string                 `json:"skipReason"`
	FailPoint           *mtest.FailPoint       `json:"failPoint"`
	ClientOptions       map[string]interface{} `json:"clientOptions"`
	SessionOptions      map[string]interface{} `json:"sessionOptions"`
	Operations          []oper                 `json:"operations"`
	Expectations        []*expectation         `json:"expectations"`
	UseMultipleMongoses bool                   `json:"useMultipleMongoses"`

	Outcome *struct {
		Collection struct {
			Name string
			Data json.RawMessage `json:"data"`
		} `json:"collection"`
	} `json:"outcome"`
}

type expectation struct {
	CommandStartedEvent struct {
		CommandName  string          `json:"command_name"`
		DatabaseName string          `json:"database_name"`
		Command      json.RawMessage `json:"command"`
	} `json:"command_started_event"`
}

type oper struct {
	Name              string                 `json:"name"`
	Object            string                 `json:"object"`
	CollectionOptions map[string]interface{} `json:"collectionOptions"`
	Result            json.RawMessage        `json:"result"`
	Arguments         json.RawMessage        `json:"arguments"`
	ArgMap            map[string]interface{}
	Error             bool `json:"error"`
}

type txnError struct {
	ErrorContains      string   `bson:"errorContains"`
	ErrorCodeName      string   `bson:"errorCodeName"`
	ErrorLabelsContain []string `bson:"errorLabelsContain"`
	ErrorLabelsOmit    []string `bson:"errorLabelsOmit"`
}

const dataPath string = "../../data/"

var directories = []string{
	"transactions",
	"crud/v2",
}

var checkOutcomeOpts = options.Collection().SetReadPreference(readpref.Primary()).SetReadConcern(readconcern.Local())

func TestNewSpecs(t *testing.T) {
	for _, specName := range directories {
		t.Run(specName, func(t *testing.T) {
			for _, fileName := range findJSONFilesInDir(t, path.Join(dataPath, specName)) {
				t.Run(fileName, func(t *testing.T) {
					runSpecTestFile(t, path.Join(dataPath, specName, fileName))
				})
			}
		})
	}
}

func runSpecTestFile(t *testing.T, filePath string) {
	// setup: read test file
	content, err := ioutil.ReadFile(filePath)
	assert.Nil(t, err, "unable to read spec test file at: %v", filePath)

	var testFile testFile
	err = json.Unmarshal(content, &testFile)
	assert.Nil(t, err, "unable to unmarshall spec test file at; %v", filePath)

	// create mtest wrapper and skip if needed
	mt := mtest.New(t, mtest.NewOptions().Constraints(testFile.RunOn...).CreateClient(false))

	for _, test := range testFile.Tests {
		runSpecTestCase(mt, test, testFile)
	}
}

func runSpecTestCase(mt *mtest.T, test testCase, testFile testFile) {
	clientOpts := createClientOptions(test.ClientOptions)
	opts := mtest.NewOptions().DatabaseName(testFile.DatabaseName).CollectionName(testFile.CollectionName).ClientOptions(clientOpts)

	if mt.TopologyKind() == mtest.Sharded && !test.UseMultipleMongoses {
		// pin to a single mongos
		opts = opts.ClientType(mtest.Pinned)
	}

	mt.RunOpts(test.Description, opts, func(mt *mtest.T) {
		if len(test.SkipReason) > 0 {
			mt.Skip(test.SkipReason)
		}

		// defer killSessions to ensure it runs regardless of the state of the test because the client has already
		// been created and the collection drop in mongotest will hang for transactions to be aborted (60 seconds)
		// in error cases.
		defer killSessions(mt)
		setupCollection(mt, testFile)

		// reset the client so all sessions used in operations have a starting txnNumber of 0
		// do not call mt.ClearCollections because we do not want to clear the test data.
		mt.ResetClient(nil)

		// create sessions, fail points, and collection
		sess0, sess1 := setupSessions(mt, test)
		// TODO: lsids
		if sess0 != nil {
			defer func() {
				sess0.EndSession(mtest.Background)
				sess1.EndSession(mtest.Background)
			}()
		}
		if test.FailPoint != nil {
			mt.SetFailPoint(*test.FailPoint)
		}

		// run operations
		mt.ClearEvents()
		for _, op := range test.Operations {
			runOperation(mt, op, sess0, sess1)
		}

		// Needs to be done here (in spite of defer) because some tests
		// require end session to be called before we check expectation
		sess0.EndSession(mtest.Background)
		sess1.EndSession(mtest.Background)
		mt.ClearFailPoints()

		// TODO: lsids
		checkExpectations(mt, test.Expectations, bsonx.Doc{}, bsonx.Doc{})

		if test.Outcome != nil {
			coll := mt.Coll
			if test.Outcome.Collection.Name != "" {
				coll = mt.CreateCollection(test.Outcome.Collection.Name, false)
			}

			// Verify with primary read pref
			coll, err := coll.Clone(checkOutcomeOpts)
			assert.Nil(mt, err, "clone error: %v", err)
			cur, err := coll.Find(mtest.Background, bson.D{})
			assert.Nil(mt, err, "Find error: %v", err)

			verifyCursorResult(mt, cur, test.Outcome.Collection.Data)
		}
	})
}

func runOperation(mt *mtest.T, op oper, sess0, sess1 mongo.Session) {
	if op.Name == "count" {
		mt.Skip("count has been deprecated")
	}

	// Arguments aren't marshaled directly into a map because runcommand
	// needs to convert them into BSON docs.  We convert them to a map here
	// for getting the session and for all other collection operations
	op.ArgMap = createArgMap(mt, op.Arguments)

	var sess mongo.Session
	if sessStr, ok := op.ArgMap["session"]; ok {
		switch sessStr.(string) {
		case "session0":
			sess = sess0
		case "session1":
			sess = sess1
		}
	}

	if op.Object == "testRunner" {
		executeTestRunnerOperation(mt, op, sess)
		return
	}

	collOpts := createCollectionOptions(op.CollectionOptions)
	if collOpts != nil {
		mt.CloneCollection(collOpts)
	}

	// execute the command on the given object
	var err error
	switch op.Object {
	case "session0":
		err = executeSessionOperation(mt, op, sess0)
	case "session1":
		err = executeSessionOperation(mt, op, sess1)
	case "collection":
		err = executeCollectionOperation(mt, op, sess)
	case "database":
		err = executeDatabaseOperation(mt, op, sess)
	}

	// ensure error is what we expect
	verifyError(mt, err, op.Result)
	if op.Error {
		assert.NotNil(mt, err, "expected error but got nil")
	}
}

func executeTestRunnerOperation(mt *mtest.T, op oper, sess mongo.Session) {
	// TODO pinned server stuff
	switch op.Name {
	case "targetedFailPoint":
		failPtStr, ok := op.ArgMap["failPoint"]
		assert.True(mt, ok, "failPoint not found in op.ArgMap")
		var fp mtest.FailPoint
		marshaled, err := json.Marshal(failPtStr.(map[string]interface{}))
		assert.Nil(mt, err, "json.Marshal error: %v", err)
		err = json.Unmarshal(marshaled, &fp)
		assert.Nil(mt, err, "json.Unmarshal error: %v", err)

		mt.SetFailPoint(fp)
	//case "assertSessionPinned":
	//	require.NotNil(t, sess.clientSession.PinnedServer)
	//case "assertSessionUnpinned":
	//	require.Nil(t, sess.clientSession.PinnedServer)
	//case "assertSessionDirty":
	//	require.NotNil(t, sess.clientSession.Server)
	//	require.True(t, sess.clientSession.Server.Dirty)
	//case "assertSessionNotDirty":
	//	require.NotNil(t, sess.clientSession.Server)
	//	require.False(t, sess.clientSession.Server.Dirty)
	//case "assertSameLsidOnLastTwoCommands":
	//	require.True(t, sameLsidOnLastTwoCommandEvents(t))
	//case "assertDifferentLsidOnLastTwoCommands":
	//	require.False(t, sameLsidOnLastTwoCommandEvents(t))
	default:
		mt.Fatalf("unknown operation %v", op.Name)
	}
}

func executeSessionOperation(mt *mtest.T, op oper, sess mongo.Session) error {
	switch op.Name {
	case "startTransaction":
		var txnOpts *options.TransactionOptions
		if opts, ok := op.ArgMap["options"]; ok {
			txnOpts = createTransactionOptions(opts.(map[string]interface{}))
		}
		return sess.StartTransaction(txnOpts)
	case "commitTransaction":
		return sess.CommitTransaction(mtest.Background)
	case "abortTransaction":
		return sess.AbortTransaction(mtest.Background)
	case "withTransaction":
		// TODO: with trans
	case "endSession":
		sess.EndSession(mtest.Background)
		return nil
	default:
		mt.Fatalf("unknown operation: %v", op.Name)
	}
	return nil
}

func executeCollectionOperation(mt *mtest.T, op oper, sess mongo.Session) error {
	switch op.Name {
	case "countDocuments":
		// no results to verify with count
		_, err := executeCountDocuments(mt, sess, op.ArgMap)
		return err
	case "distinct":
		res, err := executeDistinct(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyDistinctResult(mt, res, op.Result)
		}
		return err
	case "insertOne":
		res, err := executeInsertOne(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyInsertOneResult(mt, res, op.Result)
		}
		return err
	case "insertMany":
		res, err := executeInsertMany(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyInsertManyResult(mt, res, op.Result)
		}
		return err
	case "find":
		res, err := executeFind(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyCursorResult(mt, res, op.Result)
		}
		return err
	case "findOneAndDelete":
		res := executeFindOneAndDelete(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && res.Err() == nil {
			verifySingleResult(mt, res, op.Result)
		}
		return res.Err()
	case "findOneAndUpdate":
		res := executeFindOneAndUpdate(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && res.Err() == nil {
			verifySingleResult(mt, res, op.Result)
		}
		return res.Err()
	case "findOneAndReplace":
		res := executeFindOneAndReplace(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && res.Err() == nil {
			verifySingleResult(mt, res, op.Result)
		}
		return res.Err()
	case "deleteOne":
		res, err := executeDeleteOne(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyDeleteResult(mt, res, op.Result)
		}
		return err
	case "deleteMany":
		res, err := executeDeleteMany(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyDeleteResult(mt, res, op.Result)
		}
		return err
	case "updateOne":
		res, err := executeUpdateOne(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyUpdateResult(mt, res, op.Result)
		}
		return err
	case "updateMany":
		res, err := executeUpdateMany(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyUpdateResult(mt, res, op.Result)
		}
		return err
	case "replaceOne":
		res, err := executeReplaceOne(mt, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyUpdateResult(mt, res, op.Result)
		}
		return err
	case "aggregate":
		res, err := executeAggregate(mt, mt.Coll, sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyCursorResult(mt, res, op.Result)
		}
		return err
	case "bulkWrite":
		// TODO reenable when bulk writes implemented
		mt.Skip("Skipping until bulk writes implemented")
	default:
		mt.Fatalf("unknown collection operation: %v", op.Name)
	}
	return nil
}

func executeDatabaseOperation(mt *mtest.T, op oper, sess mongo.Session) error {
	switch op.Name {
	case "runCommand":
		var result bson.Raw
		err := executeRunCommand(mt, sess, op.ArgMap, op.Arguments).Decode(&result)
		if !resultHasError(mt, op.Result) {
			verifyRunCommandResult(mt, op.Result, result)
		}
		return err
	case "aggregate":
		res, err := executeAggregate(mt, mt.Coll.Database(), sess, op.ArgMap)
		if !resultHasError(mt, op.Result) && err == nil {
			verifyCursorResult(mt, res, op.Result)
		}
		return err
	default:
		mt.Fatalf("unknown operation: %v", op.Name)
	}
	return nil
}

func setupSessions(mt *mtest.T, test testCase) (mongo.Session, mongo.Session) {
	mt.Helper()
	var sess0Opts, sess1Opts *options.SessionOptions
	if opts, ok := test.SessionOptions["session0"]; ok {
		sess0Opts = createSessionOptions(opts.(map[string]interface{}))
	}
	if opts, ok := test.SessionOptions["session1"]; ok {
		sess1Opts = createSessionOptions(opts.(map[string]interface{}))
	}

	sess0, err := mt.Client.StartSession(sess0Opts)
	assert.Nil(mt, err, "error creating session0: %v", err)
	sess1, err := mt.Client.StartSession(sess1Opts)
	assert.Nil(mt, err, "error creating session1: %v", err)

	return sess0, sess1
}

func setupCollection(mt *mtest.T, testFile testFile) {
	mt.Helper()
	insertColl, err := mt.Coll.Clone(options.Collection().SetWriteConcern(mtest.MajorityWc))
	assert.Nil(mt, err, "unable to clone collection: %v", err)
	err = insertColl.Database().RunCommand(mtest.Background, bson.D{{"create", insertColl.Name()}}).Err()
	assert.Nil(mt, err, "error creating collection: %v", err)

	docsToInsert := docSliceToInterfaceSlice(docSliceFromRaw(mt, testFile.Data))
	if len(docsToInsert) > 0{
		_, err = insertColl.InsertMany(mtest.Background, docsToInsert)
		assert.Nil(mt, err, "unable to insert documents into collection: %v", err)
	}
}
