package integration

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type resumeType int

const (
	errorInterrupted int32 = 11601
	errorCappedPositionLost int32 = 136
	errorCursorKilled int32 = 237
	minChangeStreamVersion = "3.6.0"
	minPbrtVersion = "4.0.7"
	minStartAfterVersion = "4.1.1"

	startAfter resumeType = iota
	resumeAfter
	operationTime
)

func TestChangeStream(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().Constraints(mtest.RunOnBlock{
		MinServerVersion: minChangeStreamVersion,
		Topology: []mtest.TopologyKind{mtest.ReplicaSet},
	}).CreateClient(false))

	mt.Run("track resume token", func(mt *mtest.T) {
		// ChangeStream must continuously track the last seen resumeToken

		cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{})
		assert.Nil(mt, err, "error creating change stream: %v", err)
		defer closeStream(cs)

		generateEvents(mt, 1)
		assert.True(mt, cs.Next(mtest.Background), "expected next to return true but got false")
		assert.NotNil(mt, cs.ResumeToken(), "expected resume token but got nil")
	})
	mt.Run("missing resume token", func(mt *mtest.T) {
		// ChangeStream will throw an exception if the server response is missing the resume token

		projectDoc := bson.D{
			{"$project", bson.D{
				{"_id", 0},
			}},
		}
		cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{projectDoc})
		assert.Nil(mt, err, "error creating change stream: %v", err)
		defer closeStream(cs)

		generateEvents(mt, 1)
		assert.False(mt, cs.Next(mtest.Background), "expected Next to return false but got true")
		assert.NotNil(mt, cs.Err(), "expected error for missing resume token but got nil")
	})
	mt.Run("resume once", func(mt *mtest.T) {
		// ChangeStream will automatically resume one time on a resumable error

		cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{})
		assert.Nil(mt, err, "error creating change stream: %v", err)
		defer closeStream(cs)

		generateEvents(mt, 1)
		// kill cursor to force resumable error
		killChangeStreamCursor(mt, cs)

		mt.ClearEvents()
		// change stream should resume once and get new change
		assert.True(mt, cs.Next(mtest.Background), "expected Next to return true but got false")
		// Next should cause getMore, killCursors, and aggregate to run
		assert.NotNil(mt, mt.GetStartedEvent(), "expected getMore event but got nil")
		assert.NotNil(mt, mt.GetStartedEvent(), "expected killCursors event but got nil")
		aggEvent := mt.GetStartedEvent()
		assert.NotNil(mt, aggEvent, "expected aggregate event but got nil")
		assert.Equal(mt, "aggregate", aggEvent.CommandName,
			"command name mismatch; expected 'aggregate', got '%s'", aggEvent.CommandName)
	})
	mt.RunOpts("no resume for aggregate errors", mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
		// ChangeStream will not attempt to resume on any error encountered while executing an aggregate command

		// aggregate response: empty batch but valid cursor ID
		// getMore response: resumable error
		// killCursors response: success
		// resumed aggregate response: error
		ns := mt.Coll.Database().Name() + "." + mt.Coll.Name()
		aggRes := mtest.CreateCursorResponse(1, ns, mtest.FirstBatch)
		getMoreRes := mtest.CreateCommandErrorResponse(errorInterrupted + 1, "foo", "bar")
		killCursorsRes := mtest.CreateSuccessResponse()
		resumedAggRes := mtest.CreateCommandErrorResponse(1, "fooo", "bar")
		mt.AddMockResponses(aggRes, getMoreRes, killCursorsRes, resumedAggRes)

		cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{})
		assert.Nil(mt, err, "error creating change stream: %v", err)
		defer closeStream(cs)

		assert.False(mt, cs.Next(mtest.Background), "expected Next to return false but got true")
	})
	mt.RunOpts("no resume for non-resumable errors", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
		// ChangeStream will not attempt to resume after encountering error code 11601 (Interrupted),
		// 136 (CappedPositionLost), or 237 (CursorKilled) while executing a getMore command.

		var testCases = []struct {
			name    string
			errCode int32
		}{
			{"interrupted", errorInterrupted},
			{"capped position lost", errorCappedPositionLost},
			{"cursor killed", errorCursorKilled},
		}

		mockOpts := mtest.NewOptions().ClientType(mtest.Mock)
		for _, tc := range testCases {
			mt.RunOpts(tc.name, mockOpts, func (mt *mtest.T) {
				// aggregate response: empty batch but valid cursor ID
				// getMore response: error
				ns := mt.Coll.Database().Name() + "." + mt.Coll.Name()
				aggRes := mtest.CreateCursorResponse(1, ns, mtest.FirstBatch)
				getMoreRes := mtest.CreateCommandErrorResponse(tc.errCode, "foo", "bar")
				mt.AddMockResponses(aggRes, getMoreRes)

				cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{})
				assert.Nil(mt, err, "error creating change stream: %v", err)
				defer closeStream(cs)

				assert.False(mt, cs.Next(mtest.Background), "expected Next to return false but got true")
				err = cs.Err()
				assert.NotNil(mt, err, "expected change stream error but got nil")
				cmdErr, ok := err.(mongo.CommandError)
				assert.True(mt, ok, "expected mongo.CommandError but got %v of type %v", err, err)
				assert.Equal(mt, tc.errCode, cmdErr.Code,
					"error code mismatch; expected %v, got %v", tc.errCode, cmdErr.Code)
			})
		}
	})
	mt.RunOpts("server selection before resume", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
		// ChangeStream will perform server selection before attempting to resume, using initial readPreference
		mt.Skip("skipping for lack of SDAM monitoring")
	})
	mt.Run("empty batch cursor not closed", func(mt *mtest.T) {
		// Ensure that a cursor returned from an aggregate command with a cursor id and an initial empty batch is not closed

		cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{})
		assert.Nil(mt, err, "error creating change stream: %v", err)
		defer closeStream(cs)
		assert.True(mt, cs.ID() > 0, "expected non-zero ID but got 0")
	})
	mt.RunOpts("ignore errors from killCursors", mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
		// The killCursors command sent during the "Resume Process" must not be allowed to throw an exception.

		ns := mt.Coll.Database().Name() + "." + mt.Coll.Name()
		aggRes := mtest.CreateCursorResponse(1, ns, mtest.FirstBatch)
		getMoreRes := mtest.CreateCommandErrorResponse(errorInterrupted + 1, "foo", "bar")
		killCursorsRes := mtest.CreateCommandErrorResponse(errorInterrupted, "foo", "bar") // killCursors error
		changeDoc := bson.D{{"_id", bson.D{{"x", 1}}}}
		resumedAggRes := mtest.CreateCursorResponse(1, ns, mtest.FirstBatch, changeDoc)
		mt.AddMockResponses(aggRes, getMoreRes, killCursorsRes, resumedAggRes)

		cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{})
		assert.Nil(mt, err, "error creating change stream: %v", err)
		defer closeStream(cs)

		assert.True(mt, cs.Next(mtest.Background), "expected Next to return true but got false")
		assert.Nil(mt, cs.Err(), "expected nil error but got %v", cs.Err())
	})

	startAtOpTimeOpts := mtest.NewOptions().Constraints(mtest.RunOnBlock{
		MinServerVersion: "4.0",
		MaxServerVersion: "4.0.6",
	})
	mt.RunOpts("include startAtOperationTime", startAtOpTimeOpts, func(mt *mtest.T) {
		// $changeStream stage for ChangeStream against a server >=4.0 and <4.0.7 that has not received any results yet
		// MUST include a startAtOperationTime option when resuming a changestream.

		cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{})
		assert.Nil(mt, err, "error creating change stream: %v", err)
		defer closeStream(cs)

		generateEvents(mt, 1)
		// kill cursor to force resumable error
		killChangeStreamCursor(mt, cs)

		mt.ClearEvents()
		// change stream should resume once and get new change
		assert.True(mt, cs.Next(mtest.Background), "expected Next to return true but got false")
		// Next should cause getMore, killCursors, and aggregate to run
		assert.NotNil(mt, mt.GetStartedEvent(), "expected getMore event but got nil")
		assert.NotNil(mt, mt.GetStartedEvent(), "expected killCursors event but got nil")
		aggEvent := mt.GetStartedEvent()
		assert.NotNil(mt, aggEvent, "expected aggregate event but got nil")
		assert.Equal(mt, "aggregate", aggEvent.CommandName,
			"command name mismatch; expected 'aggregate', got '%s'", aggEvent.CommandName)

		// check for startAtOperationTime in pipeline
		csStage := aggEvent.Command.Lookup("pipeline").Array().Index(0).Value().Document() // $changeStream stage
		_, err = csStage.Lookup("$changeStream").Document().LookupErr("startAtOperationTime")
		assert.Nil(mt, err, "startAtOperationTime not included in aggregate command")
	})
	mt.RunOpts("resume token", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
		// Prose tests to make assertions on resume tokens for change streams that have not done a getMore yet
		mt.RunOpts("no getMore", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
			pbrtOpts := mtest.NewOptions().Constraints(mtest.RunOnBlock{
				MinServerVersion: minPbrtVersion,
			}).CreateClient(false)
			mt.RunOpts("with PBRT support", pbrtOpts, func(mt *mtest.T) {
				testCases := []struct {
					name string
					rt resumeType
					minServerVersion string
				}{
					{"startAfter", startAfter, minStartAfterVersion},
					{"resumeAfter", resumeAfter, minPbrtVersion},
					{"neither", operationTime, minPbrtVersion},
				}

				for _, tc := range testCases {
					tcOpts := mtest.NewOptions().Constraints(mtest.RunOnBlock{
						MinServerVersion: tc.minServerVersion,
					})
					mt.RunOpts(tc.name, tcOpts, func(mt *mtest.T) {
						// create temp stream to get a resume token
						mt.ClearEvents()
						cs, err := mt.Coll.Watch(mtest.Background, mongo.Pipeline{})
						assert.Nil(mt, err, "error creating change stream: %v", err)

						// Initial resume token should equal the PBRT in the aggregate command
						pbrt, opTime := getAggregateResponseInfo(mt)
						compareResumeTokens(mt, cs, pbrt)

						numEvents := 5
						generateEvents(mt, numEvents)

						// Iterate over one event to get resume token
						assert.True(mt, cs.Next(mtest.Background), "expected Next to return true but got false")
						token := cs.ResumeToken()
						closeStream(cs)

						var numExpectedEvents int
						var initialToken bson.Raw
						var opts *options.ChangeStreamOptions
						switch tc.rt {
						case startAfter:
							numExpectedEvents = numEvents - 1
							initialToken = token
							opts = options.ChangeStream().SetStartAfter(token)
						case resumeAfter:
							numExpectedEvents = numEvents - 1
							initialToken = token
							opts = options.ChangeStream().SetResumeAfter(token)
						case operationTime:
							numExpectedEvents = numEvents
							opts = options.ChangeStream().SetStartAtOperationTime(&opTime)
						}

						// clear slate and create new change stream
						mt.ClearEvents()
						cs, err = mt.Coll.Watch(mtest.Background, mongo.Pipeline{}, opts)
						assert.Nil(mt, err, "error creating change stream: %v", err)
						defer closeStream(cs)

						aggPbrt, _ := getAggregateResponseInfo(mt)
						compareResumeTokens(mt, cs, initialToken)

						for i := 0; i < numExpectedEvents; i++ {
							assert.True(mt, cs.Next(mtest.Background),
								"expected Next to return true but got false")
							// while we're not at the last doc in the batch, the resume token should be the _id of the
							// document
							if i != numExpectedEvents - 1 {
								compareResumeTokens(mt, cs, cs.Current.Lookup("_id").Document())
							}
						}
						// at end of batch, the resume token should equal the PBRT of the aggregate
						compareResumeTokens(mt, cs, aggPbrt)
					})
				}
			})

			noPbrtOpts := mtest.NewOptions().Constraints(mtest.RunOnBlock{
				MaxServerVersion: "4.0.6",
			}).CreateClient(false)
			mt.RunOpts("without PBRT support", noPbrtOpts, func(mt *mtest.T) {
				cs, err := mt.Coll.Database().Watch(mtest.Background, mongo.Pipeline{})
				assert.Nil(mt, err, "error creating change stream: %v", err)
				defer closeStream(cs)

				compareResumeTokens(mt, cs, nil) // should be no resume token because no PBRT
				numEvents := 5
				generateEvents(mt, numEvents)
				// iterate once to get a resume token
				assert.Equal(mt, cs.Next(mtest.Background), "expected Next to return true but got false")
				token := cs.ResumeToken()
				assert.NotNil(mt, token, "expected resume token but got nil")

				testCases := []struct {
					name            string
					opts            *options.ChangeStreamOptions
					iterateStream   bool // whether or not resulting change stream should be iterated
					initialToken    bson.Raw
					numDocsExpected int
				}{
					{"resumeAfter", options.ChangeStream().SetResumeAfter(token), true, token, numEvents - 1},
					{"no options", nil, false, nil, 0},
				}
				for _, tc := range testCases {
					mt.Run(tc.name, func(mt *mtest.T) {
						cs, err := mt.Coll.Database().Watch(mtest.Background, mongo.Pipeline{}, tc.opts)
						assert.Nil(mt, err, "error creating change stream: %v", err)
						defer closeStream(cs)

						compareResumeTokens(mt, cs, tc.initialToken)
						if !tc.iterateStream {
							return
						}

						for i := 0; i < tc.numDocsExpected; i++ {
							assert.True(mt, cs.Next(mtest.Background), "expected Next to return true but got false")
							// current resume token should always equal _id of current document
							compareResumeTokens(mt, cs, cs.Current.Lookup("_id").Document())
						}
					})
				}
			})
		})
	})
}

func closeStream(cs *mongo.ChangeStream) {
	_ = cs.Close(mtest.Background)
}

func generateEvents(mt *mtest.T, numEvents int) {
	mt.Helper()
	for i := 0; i < numEvents; i++ {
		_, err := mt.Coll.InsertOne(mtest.Background, bson.D{{"x", i}})
		assert.Nil(mt, err, "error calling InsertOne: %v", err)
	}
}

func killChangeStreamCursor(mt *mtest.T, cs *mongo.ChangeStream) {
	mt.Helper()
	db := mt.Coll.Database().Client().Database("admin")
	err := db.RunCommand(mtest.Background, bson.D{
		{"killCursors", mt.Coll.Name()},
		{"cursors", bson.A{cs.ID()}},
	}).Err()
	assert.Nil(mt, err, "error killing change stream cursor: %v", err)
}

// returns pbrt, operationTime from aggregate command response
func getAggregateResponseInfo(mt *mtest.T) (bson.Raw, primitive.Timestamp) {
	mt.Helper()
	succeeded := mt.GetSucceededEvent()
	assert.NotNil(mt, succeeded, "expected success event for aggregate but got nil")
	assert.Equal(mt, "aggregate", succeeded.CommandName,
		"command name mismatch; expected 'aggregate', got '%s'", succeeded.CommandName)

	pbrt := succeeded.Reply.Lookup("cursor", "postBatchResumeToken").Document()
	optimeT, optimeI := succeeded.Reply.Lookup("operationTime").Timestamp()
	return pbrt, primitive.Timestamp{T: optimeT, I: optimeI}
}

func compareResumeTokens(mt *mtest.T, cs *mongo.ChangeStream, expected bson.Raw) {
	mt.Helper()
	assert.Equal(mt, expected, cs.ResumeToken(),
		"resume token mismatch; expected %v, got %v", expected, cs.ResumeToken())
}
