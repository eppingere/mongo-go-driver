package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math"
	"time"
)

func executeCountDocuments(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (int64, error) {
	var filter map[string]interface{}
	opts := options.Count()
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "skip":
			opts = opts.SetSkip(int64(opt.(float64)))
		case "limit":
			opts = opts.SetLimit(int64(opt.(float64)))
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	if sess != nil {
		var count int64
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var countErr error
			count, countErr = mt.Coll.CountDocuments(sc, filter, opts)
			return countErr
		})
		return count, err
	}
	return mt.Coll.CountDocuments(mtest.Background, filter, opts)
}

func executeInsertOne(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (*mongo.InsertOneResult, error) {
	document := args["document"].(map[string]interface{})

	// For some reason, the insertion document is unmarshaled with a float rather than integer,
	// but the documents that are used to initially populate the collection are unmarshaled
	// correctly with integers. To ensure that the tests can correctly compare them, we iterate
	// through the insertion document and change any valid integers stored as floats to actual
	// integers.
	replaceFloatsWithInts(document)

	if sess != nil {
		var res *mongo.InsertOneResult
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var insertErr error
			res, insertErr = mt.Coll.InsertOne(sc, document)
			return insertErr
		})
		return res, err
	}
	return mt.Coll.InsertOne(mtest.Background, document)
}

func verifyInsertOneResult(mt *mtest.T, res *mongo.InsertOneResult, result json.RawMessage) {
	expectedBytes, err := result.MarshalJSON()
	assert.Nil(mt, err, "result.MarshalJSON error: %v", err)

	var expected mongo.InsertOneResult
	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
	assert.Nil(mt, err, "JSON decoder error: %v", err)

	expectedID := expected.InsertedID
	if f, ok := expectedID.(float64); ok && f == math.Floor(f) {
		expectedID = int32(f)
	}

	if expectedID != nil {
		assert.Equal(mt, expectedID, res.InsertedID, "inserted ID mismatch; expected %v, got %v", expectedID, res.InsertedID)
	}
}

func executeInsertMany(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (*mongo.InsertManyResult, error) {
	documents := args["documents"].([]interface{})

	// For some reason, the insertion documents are unmarshaled with a float rather than
	// integer, but the documents that are used to initially populate the collection are
	// unmarshaled correctly with integers. To ensure that the tests can correctly compare
	// them, we iterate through the insertion documents and change any valid integers stored
	// as floats to actual integers.
	for i, doc := range documents {
		docM := doc.(map[string]interface{})
		replaceFloatsWithInts(docM)

		documents[i] = docM
	}

	if sess != nil {
		var res *mongo.InsertManyResult
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var insertErr error
			res, insertErr = mt.Coll.InsertMany(sc, documents)
			return insertErr
		})
		return res, err
	}
	return mt.Coll.InsertMany(mtest.Background, documents)
}

func verifyInsertManyResult(mt *mtest.T, res *mongo.InsertManyResult, result json.RawMessage) {
	expectedBytes, err := result.MarshalJSON()
	assert.Nil(mt, err, "result.MarshalJSON error: %v", err)

	var expected struct{ InsertedIds map[string]interface{} }
	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
	assert.Nil(mt, err, "JSON decoder error: %v", err)

	if expected.InsertedIds != nil {
		assert.NotNil(mt, res, "expected result but got nil")
		replaceFloatsWithInts(expected.InsertedIds)

		for _, val := range expected.InsertedIds {
			var found bool
			for _, inserted := range res.InsertedIDs {
				if val == inserted {
					found = true
					break
				}
			}
			assert.True(mt, found, "expected to find ID %v in %v", val, res.InsertedIDs)
		}
	}
}

func executeRunCommand(mt *mtest.T, sess mongo.Session, argmap map[string]interface{}, args json.RawMessage) *mongo.SingleResult {
	var cmd bson.D
	opts := options.RunCmd()
	for name, opt := range argmap {
		switch name {
		case "command":
			argBytes, err := args.MarshalJSON()
			assert.Nil(mt, err, "args.MarshalJSOn error: %v", err)

			var argCmdStruct struct {
				Cmd json.RawMessage `json:"command"`
			}
			err = json.NewDecoder(bytes.NewBuffer(argBytes)).Decode(&argCmdStruct)
			if err != nil {
				mt.Fatalf("JSON decoder error: %v", err)
			}

			err = bson.UnmarshalExtJSON(argCmdStruct.Cmd, true, &cmd)
			if err != nil {
				mt.Fatalf("bson.UnmarshalExtJSON error: %v", err)
			}
		case "readPreference":
			opts = opts.SetReadPreference(getReadPref(opt))
		}
	}

	if sess != nil {
		var sr *mongo.SingleResult
		_ = mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			sr = mt.Coll.Database().RunCommand(sc, cmd, opts)
			return nil
		})
		return sr
	}
	return mt.Coll.Database().RunCommand(mtest.Background, cmd, opts)
}

func verifyRunCommandResult(mt *mtest.T, expected json.RawMessage, actual bson.Raw) {
	if len(expected) == 0 {
		return
	}
	jsonBytes, err := expected.MarshalJSON()
	assert.Nil(mt, err, "result.MarshalJSON error: %v", err)

	var expectedRaw bson.Raw
	err = bson.UnmarshalExtJSON(jsonBytes, true, &expectedRaw)
	assert.Nil(mt, err, "error unmarshaling expected result: %v", err)

	// All runcommand results in tests are for key "n" only
	compareValues(mt, expectedRaw.Lookup("n"), actual.Lookup("n"))
}

func executeFind(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (*mongo.Cursor, error) {
	opts := options.Find()
	var filter map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "sort":
			opts = opts.SetSort(opt.(map[string]interface{}))
		case "skip":
			opts = opts.SetSkip(int64(opt.(float64)))
		case "limit":
			opts = opts.SetLimit(int64(opt.(float64)))
		case "batchSize":
			opts = opts.SetBatchSize(int32(opt.(float64)))
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	if sess != nil {
		var c *mongo.Cursor
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var findErr error
			c, findErr = mt.Coll.Find(sc, filter, opts)
			return findErr
		})
		return c, err
	}
	return mt.Coll.Find(mtest.Background, filter, opts)
}

func verifyCursorResult(mt *mtest.T, cur *mongo.Cursor, result json.RawMessage) {
	assert.NotNil(mt, cur, "expected cursor to not be nil")
	for _, expected := range docSliceFromRaw(mt, result) {
		assert.True(mt, cur.Next(mtest.Background), "expected Next to return true but got false")
		compareDocs(mt, expected, cur.Current)
	}

	assert.False(mt, cur.Next(mtest.Background), "expected Next to return false but got true")
	err := cur.Err()
	assert.Nil(mt, err, "cursor error: %v", err)
}

func executeDistinct(mt *mtest.T, sess mongo.Session, args map[string]interface{}) ([]interface{}, error) {
	var fieldName string
	var filter map[string]interface{}
	opts := options.Distinct()
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "fieldName":
			fieldName = opt.(string)
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	if sess != nil {
		var res []interface{}
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var derr error
			res, derr = mt.Coll.Distinct(sc, fieldName, filter, opts)
			return derr
		})
		return res, err
	}
	return mt.Coll.Distinct(mtest.Background, fieldName, filter, opts)
}

func verifyDistinctResult(mt *mtest.T, res []interface{}, result json.RawMessage) {
	resultBytes, err := result.MarshalJSON()
	assert.Nil(mt, err, "result.MarshalJSON error: %v", err)

	var expected []interface{}
	err = json.NewDecoder(bytes.NewBuffer(resultBytes)).Decode(&expected)
	assert.Nil(mt, err, "JSON decoder error: %v", err)

	assert.Equal(mt, len(expected), len(res), "result length mismatch; expected %d, got %d", len(expected), len(res))

	for idx := range expected {
		expectedInt := getIntFromInterface(expected[idx])
		actualInt := getIntFromInterface(res[idx])

		assert.Equal(mt, expectedInt == nil, actualInt == nil,
			"int from interface mismatch; expected %v, got %v", expected[idx], res[idx])
		if expectedInt != nil {
			assert.Equal(mt, *expectedInt, *actualInt,
				"distinct result mismatch; expected %v, got %v", *expectedInt, *actualInt)
		}
	}
}

func executeFindOneAndDelete(mt *mtest.T, sess mongo.Session, args map[string]interface{}) *mongo.SingleResult {
	opts := options.FindOneAndDelete()
	var filter map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "sort":
			opts = opts.SetSort(opt.(map[string]interface{}))
		case "projection":
			opts = opts.SetProjection(opt.(map[string]interface{}))
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	if sess != nil {
		var res *mongo.SingleResult
		_ = mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			res = mt.Coll.FindOneAndDelete(sc, filter, opts)
			return nil
		})
		return res
	}
	return mt.Coll.FindOneAndDelete(mtest.Background, filter, opts)
}

func verifySingleResult(mt *mtest.T, res *mongo.SingleResult, result json.RawMessage) {
	jsonBytes, err := result.MarshalJSON()
	assert.Nil(mt, err, "result.MarshalJSON error: %v", err)

	actual, err := res.DecodeBytes()
	if err == mongo.ErrNoDocuments {
		var expected map[string]interface{}
		err := json.NewDecoder(bytes.NewBuffer(jsonBytes)).Decode(&expected)
		assert.Nil(mt, err, "JSON decoder error: %v", err)
		assert.Nil(mt, expected, "expected empty results map but got %v", expected)
	}

	var expected bson.Raw
	err = bson.UnmarshalExtJSON(jsonBytes, true, &expected)
	assert.Nil(mt, err, "bson.UnmarshalExtJSON error for expected document: %v", err)
	assert.Equal(mt, expected, actual, "result mismatch; expected %v, got %v", expected, actual)
}

func executeFindOneAndUpdate(mt *mtest.T, sess mongo.Session, args map[string]interface{}) *mongo.SingleResult {
	opts := options.FindOneAndUpdate()
	var filter map[string]interface{}
	var update map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "update":
			update = opt.(map[string]interface{})
		case "arrayFilters":
			opts = opts.SetArrayFilters(options.ArrayFilters{
				Filters: opt.([]interface{}),
			})
		case "sort":
			opts = opts.SetSort(opt.(map[string]interface{}))
		case "projection":
			opts = opts.SetProjection(opt.(map[string]interface{}))
		case "upsert":
			opts = opts.SetUpsert(opt.(bool))
		case "returnDocument":
			switch opt.(string) {
			case "After":
				opts = opts.SetReturnDocument(options.After)
			case "Before":
				opts = opts.SetReturnDocument(options.Before)
			}
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	// For some reason, the filter and update documents are unmarshaled with floats
	// rather than integers, but the documents that are used to initially populate the
	// collection are unmarshaled correctly with integers. To ensure that the tests can
	// correctly compare them, we iterate through the filter and replacement documents and
	// change any valid integers stored as floats to actual integers.
	replaceFloatsWithInts(filter)
	replaceFloatsWithInts(update)

	if sess != nil {
		var res *mongo.SingleResult
		_ = mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			res = mt.Coll.FindOneAndUpdate(sc, filter, update, opts)
			return nil
		})
		return res
	}
	return mt.Coll.FindOneAndUpdate(mtest.Background, filter, update, opts)
}

func executeFindOneAndReplace(mt *mtest.T, sess mongo.Session, args map[string]interface{}) *mongo.SingleResult {
	opts := options.FindOneAndReplace()
	var filter map[string]interface{}
	var replacement map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "replacement":
			replacement = opt.(map[string]interface{})
		case "sort":
			opts = opts.SetSort(opt.(map[string]interface{}))
		case "projection":
			opts = opts.SetProjection(opt.(map[string]interface{}))
		case "upsert":
			opts = opts.SetUpsert(opt.(bool))
		case "returnDocument":
			switch opt.(string) {
			case "After":
				opts = opts.SetReturnDocument(options.After)
			case "Before":
				opts = opts.SetReturnDocument(options.Before)
			}
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	// For some reason, the filter and replacement documents are unmarshaled with floats
	// rather than integers, but the documents that are used to initially populate the
	// collection are unmarshaled correctly with integers. To ensure that the tests can
	// correctly compare them, we iterate through the filter and replacement documents and
	// change any valid integers stored as floats to actual integers.
	replaceFloatsWithInts(filter)
	replaceFloatsWithInts(replacement)

	if sess != nil {
		var res *mongo.SingleResult
		_ = mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			res = mt.Coll.FindOneAndReplace(sc, filter, replacement, opts)
			return nil
		})
		return res
	}
	return mt.Coll.FindOneAndReplace(mtest.Background, filter, replacement, opts)
}

func executeDeleteOne(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (*mongo.DeleteResult, error) {
	opts := options.Delete()
	var filter map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	// For some reason, the filter document is unmarshaled with floats
	// rather than integers, but the documents that are used to initially populate the
	// collection are unmarshaled correctly with integers. To ensure that the tests can
	// correctly compare them, we iterate through the filter and replacement documents and
	// change any valid integers stored as floats to actual integers.
	replaceFloatsWithInts(filter)

	if sess != nil {
		var res *mongo.DeleteResult
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var derr error
			res, derr = mt.Coll.DeleteOne(sc, filter, opts)
			return derr
		})
		return res, err
	}
	return mt.Coll.DeleteOne(mtest.Background, filter, opts)
}

func verifyDeleteResult(mt *mtest.T, res *mongo.DeleteResult, result json.RawMessage) {
	expectedBytes, err := result.MarshalJSON()
	assert.Nil(mt, err, "result.MarshalJSON error: %v", err)

	var expected mongo.DeleteResult
	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
	assert.Nil(mt, err, "JSON decoder error: %v", err)

	assert.Equal(mt, expected.DeletedCount, res.DeletedCount,
		"deleted count mismatch; expected %v, got %v", expected.DeletedCount, res.DeletedCount)
}

func executeDeleteMany(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (*mongo.DeleteResult, error) {
	opts := options.Delete()
	var filter map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	// For some reason, the filter document is unmarshaled with floats
	// rather than integers, but the documents that are used to initially populate the
	// collection are unmarshaled correctly with integers. To ensure that the tests can
	// correctly compare them, we iterate through the filter and replacement documents and
	// change any valid integers stored as floats to actual integers.
	replaceFloatsWithInts(filter)

	if sess != nil {
		var res *mongo.DeleteResult
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var derr error
			res, derr = mt.Coll.DeleteMany(sc, filter, opts)
			return derr
		})
		return res, err
	}
	return mt.Coll.DeleteMany(mtest.Background, filter, opts)
}

func executeUpdateOne(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (*mongo.UpdateResult, error) {
	opts := options.Update()
	var filter map[string]interface{}
	var update map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "update":
			update = opt.(map[string]interface{})
		case "arrayFilters":
			opts = opts.SetArrayFilters(options.ArrayFilters{Filters: opt.([]interface{})})
		case "upsert":
			opts = opts.SetUpsert(opt.(bool))
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	// For some reason, the filter and update documents are unmarshaled with floats
	// rather than integers, but the documents that are used to initially populate the
	// collection are unmarshaled correctly with integers. To ensure that the tests can
	// correctly compare them, we iterate through the filter and replacement documents and
	// change any valid integers stored as floats to actual integers.
	replaceFloatsWithInts(filter)
	replaceFloatsWithInts(update)

	if opts.Upsert == nil {
		opts = opts.SetUpsert(false)
	}
	if sess != nil {
		var res *mongo.UpdateResult
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var uerr error
			res, uerr = mt.Coll.UpdateOne(sc, filter, update, opts)
			return uerr
		})
		return res, err
	}
	return mt.Coll.UpdateOne(mtest.Background, filter, update, opts)
}

func verifyUpdateResult(mt *mtest.T, res *mongo.UpdateResult, result json.RawMessage) {
	expectedBytes, err := result.MarshalJSON()
	assert.Nil(mt, err, "result.MarshalJSON error: %v", err)

	var expected mongo.UpdateResult
	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
	assert.Nil(mt, err, "JSON decoder error: %v", err)

	assert.Equal(mt, expected.MatchedCount, res.MatchedCount,
		"matched count mismatch; expected %v, got %v", expected.MatchedCount, res.MatchedCount)
	assert.Equal(mt, expected.ModifiedCount, res.ModifiedCount,
		"modified count mismatch; expected %v, got %v", expected.ModifiedCount, res.ModifiedCount)
	assert.Equal(mt, expected.UpsertedCount, res.UpsertedCount,
		"upserted count mismatch; expected %v, got %v", expected.UpsertedCount, res.UpsertedCount)
}

func executeUpdateMany(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (*mongo.UpdateResult, error) {
	opts := options.Update()
	var filter map[string]interface{}
	var update map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "update":
			update = opt.(map[string]interface{})
		case "arrayFilters":
			opts = opts.SetArrayFilters(options.ArrayFilters{Filters: opt.([]interface{})})
		case "upsert":
			opts = opts.SetUpsert(opt.(bool))
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	// For some reason, the filter and update documents are unmarshaled with floats
	// rather than integers, but the documents that are used to initially populate the
	// collection are unmarshaled correctly with integers. To ensure that the tests can
	// correctly compare them, we iterate through the filter and replacement documents and
	// change any valid integers stored as floats to actual integers.
	replaceFloatsWithInts(filter)
	replaceFloatsWithInts(update)

	if opts.Upsert == nil {
		opts = opts.SetUpsert(false)
	}
	if sess != nil {
		var res *mongo.UpdateResult
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var uerr error
			res, uerr = mt.Coll.UpdateMany(sc, filter, update, opts)
			return uerr
		})
		return res, err
	}
	return mt.Coll.UpdateMany(mtest.Background, filter, update, opts)
}

func executeReplaceOne(mt *mtest.T, sess mongo.Session, args map[string]interface{}) (*mongo.UpdateResult, error) {
	opts := options.Replace()
	var filter map[string]interface{}
	var replacement map[string]interface{}
	for name, opt := range args {
		switch name {
		case "filter":
			filter = opt.(map[string]interface{})
		case "replacement":
			replacement = opt.(map[string]interface{})
		case "upsert":
			opts = opts.SetUpsert(opt.(bool))
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		}
	}

	// For some reason, the filter and replacement documents are unmarshaled with floats
	// rather than integers, but the documents that are used to initially populate the
	// collection are unmarshaled correctly with integers. To ensure that the tests can
	// correctly compare them, we iterate through the filter and replacement documents and
	// change any valid integers stored as floats to actual integers.
	replaceFloatsWithInts(filter)
	replaceFloatsWithInts(replacement)

	// TODO temporarily default upsert to false explicitly to make test pass
	// because we do not send upsert=false by default
	//opts = opts.SetUpsert(false)
	if opts.Upsert == nil {
		opts = opts.SetUpsert(false)
	}
	if sess != nil {
		var res *mongo.UpdateResult
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var uerr error
			res, uerr = mt.Coll.ReplaceOne(sc, filter, replacement, opts)
			return uerr
		})
		return res, err
	}
	return mt.Coll.ReplaceOne(mtest.Background, filter, replacement, opts)
}

type aggregator interface {
	Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (*mongo.Cursor, error)
}

func executeAggregate(mt *mtest.T, agg aggregator, sess mongo.Session, args map[string]interface{}) (*mongo.Cursor, error) {
	var pipeline []interface{}
	opts := options.Aggregate()
	for name, opt := range args {
		switch name {
		case "pipeline":
			pipeline = opt.([]interface{})
		case "batchSize":
			opts = opts.SetBatchSize(int32(opt.(float64)))
		case "collation":
			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
		case "maxTimeMS":
			opts = opts.SetMaxTime(time.Duration(opt.(float64)) * time.Millisecond)
		}
	}

	if sess != nil {
		var cur *mongo.Cursor
		err := mongo.WithSession(mtest.Background, sess, func(sc mongo.SessionContext) error {
			var aerr error
			cur, aerr = agg.Aggregate(sc, pipeline, opts)
			return aerr
		})
		return cur, err
	}
	return mt.Coll.Aggregate(mtest.Background, pipeline, opts)
}
