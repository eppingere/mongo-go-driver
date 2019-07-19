package integration

import (
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var consistencySupported = mtest.NewOptions().Constraints(mtest.RunOnBlock{
	MinServerVersion: "3.6.0",
	Topology: []mtest.TopologyKind{mtest.ReplicaSet, mtest.Sharded},
})
// not supported on any topo < 3.6 OR on standalones >= 3.6
var consistencyNotSupported = mtest.NewOptions().Constraints(mtest.RunOnBlock{
	MaxServerVersion: "3.5.0",
}, mtest.RunOnBlock{
	MinServerVersion: "3.6.0",
	Topology: []mtest.TopologyKind{mtest.Single},
})

type collFunction struct {
	name string
	f func(*mongo.Collection) func(mongo.SessionContext) error
}
var fooIndex = mongo.IndexModel{
	Keys: bson.D{{"foo", 1}},
	Options: options.Index().SetName("fooIndex"),
}
var manyIndexes = []mongo.IndexModel{fooIndex}
var updateDoc = bson.D{
	{"$set", bson.D{{"x", 1}}},
}

func createReadFuncMap() []collFunction {
	agg := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Aggregate(mctx, mongo.Pipeline{}); return err }
	}
	edc := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.EstimatedDocumentCount(mctx); return err }
	}
	distinct := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Distinct(mctx, "field", bson.D{}); return err }
	}
	find := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Find(mctx, bson.D{}); return err }
	}
	findOne := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { res := coll.FindOne(mctx, bson.D{}); return res.Err() }
	}

	return []collFunction{
		{"aggregate", agg},
		{"estimated documnt count", edc},
		{"distinct", distinct},
		{"find", find},
		{"find one", findOne},
	}
}

func createWriteFuncMap() []collFunction {
	io := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.InsertOne(mctx, bson.D{}); return err }
	}
	im := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.InsertMany(mctx, []interface{}{bson.D{}}); return err }
	}
	do := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.InsertMany(mctx, []interface{}{bson.D{}}); return err }
	}
	dm := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.DeleteMany(mctx, bson.D{}); return err }
	}
	uo := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.UpdateOne(mctx, bson.D{}, updateDoc); return err }
	}
	um := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.UpdateMany(mctx, bson.D{}, updateDoc); return err }
	}
	ro := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.ReplaceOne(mctx, bson.D{}, bson.D{}); return err }
	}
	fod := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { res := coll.FindOneAndDelete(mctx, bson.D{}); return res.Err() }
	}
	foReplace := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { res := coll.FindOneAndReplace(mctx, bson.D{}, bson.D{}); return res.Err() }
	}
	fou := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { res := coll.FindOneAndUpdate(mctx, bson.D{}, updateDoc); return res.Err() }
	}
	dc := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { return coll.Drop(mctx) }
	}
	dd := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		// drop a random db to avoid dropping the test database
		return func(mctx mongo.SessionContext) error { return coll.Database().Client().Database("foo").Drop(mctx) }
	}
	coi := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Indexes().CreateOne(mctx, fooIndex); return err }
	}
	cmi := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Indexes().CreateMany(mctx, manyIndexes); return err }
	}
	doi := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Indexes().DropOne(mctx, "barIndex"); return err }
	}
	dai := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Indexes().DropAll(mctx); return err }
	}
	lc := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Database().ListCollections(mctx, bson.D{}); return err }
	}
	ld := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Database().Client().ListDatabases(mctx, bson.D{}); return err }
	}
	li := func(coll *mongo.Collection) func(mongo.SessionContext) error {
		return func(mctx mongo.SessionContext) error { _, err := coll.Indexes().List(mctx); return err }
	}

	return []collFunction{
		{"insert one", io},
		{"insert many", im},
		{"delete one", do},
		{"delete many", dm},
		{"update one", uo},
		{"update many", um},
		{"replace one", ro},
		{"find one and delete", fod},
		{"find one and replace", foReplace},
		{"find one and update", fou},
		{"drop collection", dc},
		{"drop database", dd},
		{"create one index", coi},
		{"create many indexes", cmi},
		{"drop one index", doi},
		{"drop all indexes", dai},
		{"list collections", lc},
		{"list databases", ld},
		{"list indexes", li},
	}
}

func checkOperationTimeIncluded(mt *mtest.T, cmd bson.Raw, shouldInclude bool) {
	mt.Helper()
	rc, err := cmd.LookupErr("readConcern")
	assert.Nil(mt, err, "read concern not found in cmd")

	actVal, err := bson.Raw(rc.Value).LookupErr("afterClusterTime")
	if shouldInclude {
		assert.Nil(mt, err, "expected afterClusterTime in cmd but got nil")
		return
	}
	assert.NotNil(mt, err, "did not expect afterClusterTime but got %v", actVal)
}

func getOperationTime(mt *mtest.T, cmd bson.Raw) *primitive.Timestamp {
	mt.Helper()
	rc, err := cmd.LookupErr("readConcern")
	assert.Nil(mt, err, "read concern not found in cmd")

	ct, err := bson.Raw(rc.Value).LookupErr("afterClusterTime")
	assert.Nil(mt, err, "afterClusterTime not found in cmd")

	timeT, timeI := ct.Timestamp()
	return &primitive.Timestamp{
		T: timeT,
		I: timeI,
	}
}

func checkReadConcern(mt *mtest.T, cmd bson.Raw, levelIncluded bool, expectedLevel string, optimeIncluded bool, expectedTime *primitive.Timestamp) {
	mt.Helper()

	rc, err := cmd.LookupErr("readConcern")
	assert.Nil(mt, err, "read concern not found in cmd")

	rcDoc := rc.Document()
	levelVal, err := rcDoc.LookupErr("level")
	if levelIncluded {
		assert.Nil(mt, err, "level expected but not found")
		levelStr := levelVal.StringValue()
		assert.Equal(mt, expectedLevel, levelStr, "level mismatch; expected %s, got %s", expectedLevel, levelStr)
	} else {
		assert.NotNil(mt, err, "level not expected but found %v", levelVal)
	}

	ct, err := rcDoc.LookupErr("afterClusterTime")
	if optimeIncluded {
		assert.Nil(mt, err, "afterClusterTime expected but not found")
		ctT, ctI := ct.Timestamp()
		ctTimestamp := &primitive.Timestamp{T: ctT, I: ctI}
		assert.Equal(mt, expectedTime, ctTimestamp, "cluster time mismatch; expected %v, got %v", expectedTime, ctTimestamp)
	} else {
		assert.NotNil(mt, err, "afterClusterTime not expected but found %v", ct)
	}
}


func TestCausalConsistency(t *testing.T) {
	clientOpts := options.Client().SetReadConcern(mtest.LocalRc)
	mt := mtest.New(t, mtest.NewOptions().CreateClient(false).ClientOptions(clientOpts))

	mt.RunOpts("with consistency", consistencySupported.CreateClient(false), func(mt *mtest.T) {
		mt.Run("initial optime nil", func(mt *mtest.T) {
			// a client session has no initial operation time

			sess, err := mt.Client.StartSession()
			assert.Nil(mt, err, "error starting session: %v", err)
			defer sess.EndSession(mtest.Background)
			optime := sess.OperationTime()
			assert.Nil(mt, optime, "op time mismatch; expected nil, got %v", optime)
		})

		mt.Run("no afterClusterTime on first cmd", func(mt *mtest.T) {
			// the first read in a causally consistent session must not send afterClusterTime

			mt.ClearEvents()
			err := mt.Client.UseSessionWithOptions(mtest.Background,
				options.Session().SetCausalConsistency(true),
				func(mctx mongo.SessionContext) error {
					_, err := mt.Coll.Find(mctx, bson.D{})
					return err
				},
			)
			assert.Nil(mt, err, "callback err: %v", err)

			started := mt.GetStartedEvent()
			assert.NotNil(mt, started, "expected find event but got nil")
			assert.Equal(mt, "find", started.CommandName,
				"cmd name mismatch; expected 'find', got '%s'", started.CommandName)
			checkOperationTimeIncluded(mt, started.Command, false)
		})

		mt.Run("operation time updated", func(mt *mtest.T) {
			// the first read or write on a ClientSession should update the operationTime of the client session

			sess, err := mt.Client.StartSession()
			assert.Nil(mt, err, "error creating session: %v", err)
			defer sess.EndSession(mtest.Background)

			// ignore errors because operation time should be updated regardless
			_ = mongo.WithSession(mtest.Background, sess, func(mctx mongo.SessionContext) error {
				_, _ = mt.Coll.Find(mctx, bson.D{})
				return nil
			})

			serverT, serverI := mt.GetSucceededEvent().Reply.Lookup("operationTime").Timestamp()
			serverOptime := &primitive.Timestamp{T: serverT, I: serverI}
			sessOptime := sess.OperationTime()
			assert.Equal(mt, serverOptime, sessOptime, "op time mismatch; expected %v, got %v", serverOptime, sessOptime)
		})
		mt.RunOpts("operation time sent", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
			// A findOne followed by any other read operation should include the operationTime returned by the server
			// for the first operation in the afterClusterTime parameter of the second operation

			for _, collFn := range createReadFuncMap() {
				mt.Run(collFn.name, func(mt *mtest.T) {
					sess, err := mt.Client.StartSession()
					assert.Nil(mt, err, "error creating session: %v", err)
					defer sess.EndSession(mtest.Background)

					_ = mongo.WithSession(mtest.Background, sess, func(mctx mongo.SessionContext) error {
						_, _ = mt.Coll.Find(mctx, bson.D{})
						return nil
					})
					assert.Nil(mt, err, "findOne error: %v", err)
					currOptime := sess.OperationTime() // optime returned by server for findOne

					mt.ClearEvents()
					// ignore command errors
					_ = mongo.WithSession(mtest.Background, sess, collFn.f(mt.Coll))

					sentOptime := getOperationTime(mt, mt.GetStartedEvent().Command)
					assert.Equal(mt, currOptime, sentOptime, "op time mismatch; expected %v, got %v", currOptime, sentOptime)
				})
			}
		})
		mt.RunOpts("write then read", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
			// Any write operation followed by a findOne operation should include the operationTime of the first
			// operation in the afterClusterTime parameter of the second operation

			for _, collFn := range createWriteFuncMap() {
				mt.Run(collFn.name, func(mt *mtest.T) {
					sess, err := mt.Client.StartSession()
					assert.Nil(mt, err, "error creating session: %v", err)
					defer sess.EndSession(mtest.Background)

					_ = mongo.WithSession(mtest.Background, sess, collFn.f(mt.Coll))
					currOptime := sess.OperationTime() // optime returned by server for write command

					mt.ClearEvents()
					_ = mongo.WithSession(mtest.Background, sess, func(mctx mongo.SessionContext) error {
						_ = mt.Coll.FindOne(mctx, bson.D{})
						return nil
					})

					sentOptime := getOperationTime(mt, mt.GetStartedEvent().Command)
					assert.Equal(mt, currOptime, sentOptime, "op time mismatch; expected %v, got %v", currOptime, sentOptime)
				})
			}
		})
		mt.Run("non-consistent read", func(mt *mtest.T) {
			// A read operation in a ClientSession that is not causally consistent should not include the
			// afterClusterTime parameter in the command sent to the server

			sessOpts := options.Session().SetCausalConsistency(false)
			_ = mt.Client.UseSessionWithOptions(mtest.Background, sessOpts, func(mctx mongo.SessionContext) error {
				_, _ = mt.Coll.Find(mtest.Background, bson.D{})
				return nil
			})
			checkOperationTimeIncluded(mt, mt.GetStartedEvent().Command, false)
		})
		// create client without read concern so default is used
		noRcOpts := options.Client().SetWriteConcern(mtest.MajorityWc).SetReadPreference(mtest.PrimaryRp)
		mt.RunOpts("default read concern", mtest.NewOptions().ClientOptions(noRcOpts), func(mt *mtest.T) {
			// When using the default server ReadConcern the readConcern parameter in the command sent to the server
			// should not include a level field

			sess, err := mt.Client.StartSession()
			assert.Nil(mt, err, "error creating session: %v", err)
			defer sess.EndSession(mtest.Background)

			_ = mongo.WithSession(mtest.Background, sess, func(mctx mongo.SessionContext) error {
				_ = mt.Coll.FindOne(mctx, bson.D{})
				return nil
			})
			currOptime := sess.OperationTime()
			mt.ClearEvents()
			_ = mongo.WithSession(mtest.Background, sess, func(mctx mongo.SessionContext) error {
				_ = mt.Coll.FindOne(mctx, bson.D{})
				return nil
			})

			started := mt.GetStartedEvent().Command
			checkReadConcern(mt, started, false, "", true, currOptime)
		})
		majorityRc := readconcern.Majority()
		majorityRcCollOpts := options.Collection().SetReadConcern(majorityRc)
		mt.RunOpts("non-default read concern", mtest.NewOptions().CollectionOptions(majorityRcCollOpts), func(mt *mtest.T) {
			// When using a custom ReadConcern the readConcern field in the command sent to the server should be a
			// merger of the ReadConcern value and the afterClusterTime field

			// no special setup needed because coll uses majority read concern
			sess, err := mt.Client.StartSession()
			assert.Nil(mt, err, "error creating session: %v", err)
			defer sess.EndSession(mtest.Background)

			_ = mongo.WithSession(mtest.Background, sess, func(mctx mongo.SessionContext) error {
				_ = mt.Coll.FindOne(mctx, bson.D{})
				return nil
			})
			currOptime := sess.OperationTime()
			mt.ClearEvents()
			_ = mongo.WithSession(mtest.Background, sess, func(mctx mongo.SessionContext) error {
				_ = mt.Coll.FindOne(mctx, bson.D{})
				return nil
			})

			started := mt.GetStartedEvent().Command
			checkReadConcern(mt, started, true, "majority", true, currOptime)
		})
		unackWcOpts := options.Collection().SetWriteConcern(writeconcern.New(writeconcern.W(0)))
		mt.RunOpts("unacknowledged write", mtest.NewOptions().CollectionOptions(unackWcOpts), func(mt *mtest.T) {
			// When an unacknowledged write is executed in a causally consistent ClientSession the operationTime
			// property of the ClientSession is	not updated.

			sess, err := mt.Client.StartSession()
			assert.Nil(mt, err, "error creating session: %v", err)
			defer sess.EndSession(mtest.Background)

			_ = mongo.WithSession(mtest.Background, sess, func(mctx mongo.SessionContext) error {
				_, _ = mt.Coll.InsertOne(mctx, bson.D{})
				return nil
			})
			optime := sess.OperationTime()
			assert.Nil(mt, optime, "operation time mismatch; expected nil, got %v", optime)
		})
		mt.Run("cluster time included", func(mt *mtest.T) {
			// When connected to a deployment that does support cluster times messages sent to the server should
			// include $clusterTime

			_ = mt.Client.Ping(mtest.Background, readpref.Primary())
			mt.ClearEvents()
			_ = mt.Coll.FindOne(mtest.Background, bson.D{})
			_, err := mt.GetStartedEvent().Command.LookupErr("$clusterTime")
			assert.Nil(mt, err, "expected $clusterTime but got nil")
		})
	})
	mt.RunOpts("without consistency", consistencyNotSupported.CreateClient(false), func(mt *mtest.T) {
		mt.Run("no cluster time in consistent read", func(mt *mtest.T) {
			// A read operation in a causally consistent session against a deployment that does not support cluster times
			// does not include the afterClusterTime parameter in the command sent to the server

			_ = mt.Client.UseSession(mtest.Background, func(mctx mongo.SessionContext) error {
				_, _ = mt.Coll.Find(mtest.Background, bson.D{})
				return nil
			})
			checkOperationTimeIncluded(mt, mt.GetStartedEvent().Command, false)
		})
		mt.Run("cluster time not included", func(mt *mtest.T) {
			// When connected to a deployment that does not support cluster times messages sent to the server should
			// not include $clusterTime

			_ = mt.Coll.FindOne(mtest.Background, bson.D{})
			ct, err := mt.GetStartedEvent().Command.LookupErr("$clusterTime")
			assert.NotNil(mt, err, "$clusterTime mismatch; expected nil but got %v", ct)
		})
	})
}
