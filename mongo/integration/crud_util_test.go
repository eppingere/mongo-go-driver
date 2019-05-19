package integration
//
//import (
//	"strings"
//	"testing"
//
//	"go.mongodb.org/mongo-driver/bson"
//	"go.mongodb.org/mongo-driver/internal/testutil/assert"
//	"go.mongodb.org/mongo-driver/mongo"
//	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
//	"go.mongodb.org/mongo-driver/mongo/options"
//	"go.mongodb.org/mongo-driver/mongo/readconcern"
//	"go.mongodb.org/mongo-driver/mongo/readpref"
//	"go.mongodb.org/mongo-driver/mongo/writeconcern"
//)
//
//// Various helper functions for crud related operation
//
//func readPrefFromString(s string) *readpref.ReadPref {
//	switch strings.ToLower(s) {
//	case "primary":
//		return readpref.Primary()
//	case "primarypreferred":
//		return readpref.PrimaryPreferred()
//	case "secondary":
//		return readpref.Secondary()
//	case "secondarypreferred":
//		return readpref.SecondaryPreferred()
//	case "nearest":
//		return readpref.Nearest()
//	}
//	return readpref.Primary()
//}
//
//func getReadConcern(opt interface{}) *readconcern.ReadConcern {
//	return readconcern.New(readconcern.Level(opt.(map[string]interface{})["level"].(string)))
//}
//
//func getWriteConcern(opt interface{}) *writeconcern.WriteConcern {
//	if w, ok := opt.(map[string]interface{}); ok {
//		var newTimeout time.Duration
//		if conv, ok := w["wtimeout"].(float64); ok {
//			newTimeout = time.Duration(int(conv)) * time.Millisecond
//		}
//		var newJ bool
//		if conv, ok := w["j"].(bool); ok {
//			newJ = conv
//		}
//		if conv, ok := w["w"].(string); ok && conv == "majority" {
//			return writeconcern.New(writeconcern.WMajority(), writeconcern.J(newJ), writeconcern.WTimeout(newTimeout))
//		} else if conv, ok := w["w"].(float64); ok {
//			return writeconcern.New(writeconcern.W(int(conv)), writeconcern.J(newJ), writeconcern.WTimeout(newTimeout))
//		}
//	}
//	return nil
//}
//
//func collationFromMap(m map[string]interface{}) *options.Collation {
//	var collation options.Collation
//
//	if locale, found := m["locale"]; found {
//		collation.Locale = locale.(string)
//	}
//
//	if caseLevel, found := m["caseLevel"]; found {
//		collation.CaseLevel = caseLevel.(bool)
//	}
//
//	if caseFirst, found := m["caseFirst"]; found {
//		collation.CaseFirst = caseFirst.(string)
//	}
//
//	if strength, found := m["strength"]; found {
//		collation.Strength = int(strength.(float64))
//	}
//
//	if numericOrdering, found := m["numericOrdering"]; found {
//		collation.NumericOrdering = numericOrdering.(bool)
//	}
//
//	if alternate, found := m["alternate"]; found {
//		collation.Alternate = alternate.(string)
//	}
//
//	if maxVariable, found := m["maxVariable"]; found {
//		collation.MaxVariable = maxVariable.(string)
//	}
//
//	if normalization, found := m["normalization"]; found {
//		collation.Normalization = normalization.(bool)
//	}
//
//	if backwards, found := m["backwards"]; found {
//		collation.Backwards = backwards.(bool)
//	}
//
//	return &collation
//}
//
//
//func replaceFloatsWithInts(m map[string]interface{}) {
//	for key, val := range m {
//		if f, ok := val.(float64); ok && f == math.Floor(f) {
//			m[key] = int32(f)
//			continue
//		}
//
//		if innerM, ok := val.(map[string]interface{}); ok {
//			replaceFloatsWithInts(innerM)
//			m[key] = innerM
//		}
//	}
//}
//
//func makeClientOptions(opts map[string]interface{}) options.ClientOptions {
//	opts := options.Client()
//	for name, opt := range opts {
//		switch name {
//		case "retryWrites":
//			opts = opts.SetRetryWrites(opt.(bool))
//		case "w":
//			switch opt.(type) {
//			case float64:
//				opts = opts.SetWriteConcern(writeconcern.New(writeconcern.W(int(opt.(float64)))))
//			case string:
//				opts = opts.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
//			}
//		case "readConcernLevel":
//			opts = opts.SetReadConcern(readconcern.New(readconcern.Level(opt.(string))))
//		case "readPreference":
//			opts = opts.SetReadPreference(readPrefFromString(opt.(string)))
//		}
//	}
//	return opts
//}
//
//func makeCollectionOptions(opts map[string]interface{}) options.CollectionOptions {
//	opts := options.Collection()
//	for name, opt := range opts {
//		switch name {
//		case "readConcern":
//			opts = opts.SetReadConcern(getReadConcern(opt))
//		case "writeConcern":
//			opts = opts.SetWriteConcern(getWriteConcern(opt))
//		case "readPreference":
//			opts = opts.SetReadPreference(readPrefFromString(opt.(map[string]interface{})["mode"].(string)))
//		}
//	}
//	return opts
//}
//
//func executeCount(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (int64, error) {
//	var filter map[string]interface{}
//	opts := options.Count()
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "skip":
//			opts = opts.SetSkip(int64(opt.(float64)))
//		case "limit":
//			opts = opts.SetLimit(int64(opt.(float64)))
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	if sess != nil {
//		// EXAMPLE:
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.CountDocuments(sessCtx, filter, opts)
//	}
//	return coll.CountDocuments(ctx, filter, opts)
//}
//
//func executeDistinct(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) ([]interface{}, error) {
//	var fieldName string
//	var filter map[string]interface{}
//	opts := options.Distinct()
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "fieldName":
//			fieldName = opt.(string)
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.Distinct(sessCtx, fieldName, filter, opts)
//	}
//	return coll.Distinct(ctx, fieldName, filter, opts)
//}
//
//func executeInsertOne(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.InsertOneResult, error) {
//	document := args["document"].(map[string]interface{})
//
//	// For some reason, the insertion document is unmarshaled with a float rather than integer,
//	// but the documents that are used to initially populate the collection are unmarshaled
//	// correctly with integers. To ensure that the tests can correctly compare them, we iterate
//	// through the insertion document and change any valid integers stored as floats to actual
//	// integers.
//	replaceFloatsWithInts(document)
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.InsertOne(sessCtx, document)
//	}
//	return coll.InsertOne(context.Background(), document)
//}
//
//func executeInsertMany(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.InsertManyResult, error) {
//	documents := args["documents"].([]interface{})
//
//	// For some reason, the insertion documents are unmarshaled with a float rather than
//	// integer, but the documents that are used to initially populate the collection are
//	// unmarshaled correctly with integers. To ensure that the tests can correctly compare
//	// them, we iterate through the insertion documents and change any valid integers stored
//	// as floats to actual integers.
//	for i, doc := range documents {
//		docM := doc.(map[string]interface{})
//		replaceFloatsWithInts(docM)
//
//		documents[i] = docM
//	}
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.InsertMany(sessCtx, documents)
//	}
//	return coll.InsertMany(context.Background(), documents)
//}
//
//func executeFind(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.Cursor, error) {
//	opts := options.Find()
//	var filter map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "sort":
//			opts = opts.SetSort(opt.(map[string]interface{}))
//		case "skip":
//			opts = opts.SetSkip(int64(opt.(float64)))
//		case "limit":
//			opts = opts.SetLimit(int64(opt.(float64)))
//		case "batchSize":
//			opts = opts.SetBatchSize(int32(opt.(float64)))
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.Find(sessCtx, filter, opts)
//	}
//	return coll.Find(ctx, filter, opts)
//}
//
//func executeFindOneAndDelete(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) *mongo.SingleResult {
//	opts := options.FindOneAndDelete()
//	var filter map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "sort":
//			opts = opts.SetSort(opt.(map[string]interface{}))
//		case "projection":
//			opts = opts.SetProjection(opt.(map[string]interface{}))
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.FindOneAndDelete(sessCtx, filter, opts)
//	}
//	return coll.FindOneAndDelete(ctx, filter, opts)
//}
//
//func executeFindOneAndUpdate(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) *SingleResult {
//	opts := options.FindOneAndUpdate()
//	var filter map[string]interface{}
//	var update map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "update":
//			update = opt.(map[string]interface{})
//		case "arrayFilters":
//			opts = opts.SetArrayFilters(options.ArrayFilters{
//				Filters: opt.([]interface{}),
//			})
//		case "sort":
//			opts = opts.SetSort(opt.(map[string]interface{}))
//		case "projection":
//			opts = opts.SetProjection(opt.(map[string]interface{}))
//		case "upsert":
//			opts = opts.SetUpsert(opt.(bool))
//		case "returnDocument":
//			switch opt.(string) {
//			case "After":
//				opts = opts.SetReturnDocument(options.After)
//			case "Before":
//				opts = opts.SetReturnDocument(options.Before)
//			}
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	// For some reason, the filter and update documents are unmarshaled with floats
//	// rather than integers, but the documents that are used to initially populate the
//	// collection are unmarshaled correctly with integers. To ensure that the tests can
//	// correctly compare them, we iterate through the filter and replacement documents and
//	// change any valid integers stored as floats to actual integers.
//	replaceFloatsWithInts(filter)
//	replaceFloatsWithInts(update)
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.FindOneAndUpdate(sessCtx, filter, update, opts)
//	}
//	return coll.FindOneAndUpdate(ctx, filter, update, opts)
//}
//
//func executeFindOneAndReplace(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) *mongo.SingleResult {
//	opts := options.FindOneAndReplace()
//	var filter map[string]interface{}
//	var replacement map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "replacement":
//			replacement = opt.(map[string]interface{})
//		case "sort":
//			opts = opts.SetSort(opt.(map[string]interface{}))
//		case "projection":
//			opts = opts.SetProjection(opt.(map[string]interface{}))
//		case "upsert":
//			opts = opts.SetUpsert(opt.(bool))
//		case "returnDocument":
//			switch opt.(string) {
//			case "After":
//				opts = opts.SetReturnDocument(options.After)
//			case "Before":
//				opts = opts.SetReturnDocument(options.Before)
//			}
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	// For some reason, the filter and replacement documents are unmarshaled with floats
//	// rather than integers, but the documents that are used to initially populate the
//	// collection are unmarshaled correctly with integers. To ensure that the tests can
//	// correctly compare them, we iterate through the filter and replacement documents and
//	// change any valid integers stored as floats to actual integers.
//	replaceFloatsWithInts(filter)
//	replaceFloatsWithInts(replacement)
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.FindOneAndReplace(sessCtx, filter, replacement, opts)
//	}
//	return coll.FindOneAndReplace(ctx, filter, replacement, opts)
//}
//
//func executeDeleteOne(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.DeleteResult, error) {
//	opts := options.Delete()
//	var filter map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	// For some reason, the filter document is unmarshaled with floats
//	// rather than integers, but the documents that are used to initially populate the
//	// collection are unmarshaled correctly with integers. To ensure that the tests can
//	// correctly compare them, we iterate through the filter and replacement documents and
//	// change any valid integers stored as floats to actual integers.
//	replaceFloatsWithInts(filter)
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.DeleteOne(sessCtx, filter, opts)
//	}
//	return coll.DeleteOne(ctx, filter, opts)
//}
//
//func executeDeleteMany(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.DeleteResult, error) {
//	opts := options.Delete()
//	var filter map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	// For some reason, the filter document is unmarshaled with floats
//	// rather than integers, but the documents that are used to initially populate the
//	// collection are unmarshaled correctly with integers. To ensure that the tests can
//	// correctly compare them, we iterate through the filter and replacement documents and
//	// change any valid integers stored as floats to actual integers.
//	replaceFloatsWithInts(filter)
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.DeleteMany(sessCtx, filter, opts)
//	}
//	return coll.DeleteMany(ctx, filter, opts)
//}
//
//func executeReplaceOne(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.UpdateResult, error) {
//	opts := options.Replace()
//	var filter map[string]interface{}
//	var replacement map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "replacement":
//			replacement = opt.(map[string]interface{})
//		case "upsert":
//			opts = opts.SetUpsert(opt.(bool))
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	// For some reason, the filter and replacement documents are unmarshaled with floats
//	// rather than integers, but the documents that are used to initially populate the
//	// collection are unmarshaled correctly with integers. To ensure that the tests can
//	// correctly compare them, we iterate through the filter and replacement documents and
//	// change any valid integers stored as floats to actual integers.
//	replaceFloatsWithInts(filter)
//	replaceFloatsWithInts(replacement)
//
//	// TODO temporarily default upsert to false explicitly to make test pass
//	// because we do not send upsert=false by default
//	//opts = opts.SetUpsert(false)
//	if opts.Upsert == nil {
//		opts = opts.SetUpsert(false)
//	}
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.ReplaceOne(sessCtx, filter, replacement, opts)
//	}
//	return coll.ReplaceOne(ctx, filter, replacement, opts)
//}
//
//func executeUpdateOne(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.UpdateResult, error) {
//	opts := options.Update()
//	var filter map[string]interface{}
//	var update map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "update":
//			update = opt.(map[string]interface{})
//		case "arrayFilters":
//			opts = opts.SetArrayFilters(options.ArrayFilters{Filters: opt.([]interface{})})
//		case "upsert":
//			opts = opts.SetUpsert(opt.(bool))
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	// For some reason, the filter and update documents are unmarshaled with floats
//	// rather than integers, but the documents that are used to initially populate the
//	// collection are unmarshaled correctly with integers. To ensure that the tests can
//	// correctly compare them, we iterate through the filter and replacement documents and
//	// change any valid integers stored as floats to actual integers.
//	replaceFloatsWithInts(filter)
//	replaceFloatsWithInts(update)
//
//	if opts.Upsert == nil {
//		opts = opts.SetUpsert(false)
//	}
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.UpdateOne(sessCtx, filter, update, opts)
//	}
//	return coll.UpdateOne(ctx, filter, update, opts)
//}
//
//func executeUpdateMany(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.UpdateResult, error) {
//	opts := options.Update()
//	var filter map[string]interface{}
//	var update map[string]interface{}
//	for name, opt := range args {
//		switch name {
//		case "filter":
//			filter = opt.(map[string]interface{})
//		case "update":
//			update = opt.(map[string]interface{})
//		case "arrayFilters":
//			opts = opts.SetArrayFilters(options.ArrayFilters{Filters: opt.([]interface{})})
//		case "upsert":
//			opts = opts.SetUpsert(opt.(bool))
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		}
//	}
//
//	// For some reason, the filter and update documents are unmarshaled with floats
//	// rather than integers, but the documents that are used to initially populate the
//	// collection are unmarshaled correctly with integers. To ensure that the tests can
//	// correctly compare them, we iterate through the filter and replacement documents and
//	// change any valid integers stored as floats to actual integers.
//	replaceFloatsWithInts(filter)
//	replaceFloatsWithInts(update)
//
//	if opts.Upsert == nil {
//		opts = opts.SetUpsert(false)
//	}
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.UpdateMany(sessCtx, filter, update, opts)
//	}
//	return coll.UpdateMany(ctx, filter, update, opts)
//}
//
//func executeAggregate(sess mongo.Session, coll *mongo.Collection, args map[string]interface{}) (*mongo.Cursor, error) {
//	var pipeline []interface{}
//	opts := options.Aggregate()
//	for name, opt := range args {
//		switch name {
//		case "pipeline":
//			pipeline = opt.([]interface{})
//		case "batchSize":
//			opts = opts.SetBatchSize(int32(opt.(float64)))
//		case "collation":
//			opts = opts.SetCollation(collationFromMap(opt.(map[string]interface{})))
//		case "maxTimeMS":
//			opts = opts.SetMaxTime(time.Duration(opt.(float64)) * time.Millisecond)
//		}
//	}
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return coll.Aggregate(sessCtx, pipeline, opts)
//	}
//	return coll.Aggregate(ctx, pipeline, opts)
//}
//
//func executeWithTransaction(t *mtest.T, sess mongo.Session, collName string, db *mongo.Database, args json.RawMessage) error {
//	expectedBytes, err := args.MarshalJSON()
//	if err != nil {
//		return err
//	}
//
//	var testArgs withTransactionArgs
//	err = json.Unmarshal(expectedBytes, &testArgs)
//	if err != nil {
//		return err
//	}
//	opts := getTransactionOptions(testArgs.Options)
//
//	_, err = sess.WithTransaction(context.Background(), func(sessCtx SessionContext) (interface{}, error) {
//		err := runWithTransactionOperations(t, testArgs.Callback.Operations, sess, collName, db)
//		return nil, err
//	}, opts)
//	return err
//}
//
//func executeRenameCollection(sess mongo.Session, coll *mongo.Collection, argmap map[string]interface{}) *mongo.SingleResult {
//	to := argmap["to"].(string)
//
//	cmd := bson.D{
//		{"renameCollection", strings.Join([]string{coll.Database().Name(), coll.Name()}, ".")},
//		{"to", strings.Join([]string{coll.Database().Name(), to}, ".")},
//	}
//
//	admin := coll.Database().Client().Database("admin")
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return admin.RunCommand(sessCtx, cmd)
//	}
//
//	return admin.RunCommand(ctx, cmd)
//}
//
//func executeRunCommand(sess mongo.Session, db *mongo.Database, argmap map[string]interface{}, args json.RawMessage) *mongo.SingleResult {
//	var cmd bsonx.Doc
//	opts := options.RunCmd()
//	for name, opt := range argmap {
//		switch name {
//		case "command":
//			argBytes, err := args.MarshalJSON()
//			if err != nil {
//				return &SingleResult{err: err}
//			}
//
//			var argCmdStruct struct {
//				Cmd json.RawMessage `json:"command"`
//			}
//			err = json.NewDecoder(bytes.NewBuffer(argBytes)).Decode(&argCmdStruct)
//			if err != nil {
//				return &SingleResult{err: err}
//			}
//
//			err = bson.UnmarshalExtJSON(argCmdStruct.Cmd, true, &cmd)
//			if err != nil {
//				return &SingleResult{err: err}
//			}
//		case "readPreference":
//			opts = opts.SetReadPreference(getReadPref(opt))
//		}
//	}
//
//	if sess != nil {
//		sessCtx := sessionContext{
//			Context: context.WithValue(ctx, sessionKey{}, sess),
//			Session: sess,
//		}
//		return db.RunCommand(sessCtx, cmd, opts)
//	}
//	return db.RunCommand(ctx, cmd, opts)
//}
//
//func verifyBulkWriteResult(t *mtest.T, res *mongo.BulkWriteResult, result json.RawMessage) {
//	expectedBytes, err := result.MarshalJSON()
//	assert.Nil(t, err, "Error marshaling JSON: %v", err)
//
//	var expected BulkWriteResult
//	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
//	assert.Nil(t, err, "Error decoding result: %v", err)
//
//	assert.Equal(t, expected.DeletedCount, res.DeletedCount)
//	assert.Equal(t, expected.InsertedCount, res.InsertedCount)
//	assert.Equal(t, expected.MatchedCount, res.MatchedCount)
//	assert.Equal(t, expected.ModifiedCount, res.ModifiedCount)
//	assert.Equal(t, expected.UpsertedCount, res.UpsertedCount)
//
//	// replace floats with ints
//	for opID, upsertID := range expected.UpsertedIDs {
//		if floatID, ok := upsertID.(float64); ok {
//			expected.UpsertedIDs[opID] = int32(floatID)
//		}
//	}
//
//	for operationID, upsertID := range expected.UpsertedIDs {
//		assert.Equal(t, upsertID, res.UpsertedIDs[operationID])
//	}
//}
//
//func verifyInsertOneResult(t *mtest.T, res *mongo.InsertOneResult, result json.RawMessage) {
//	expectedBytes, err := result.MarshalJSON()
//	assert.Nil(t, err)
//
//	var expected InsertOneResult
//	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
//	assert.Nil(t, err)
//
//	expectedID := expected.InsertedID
//	if f, ok := expectedID.(float64); ok && f == math.Floor(f) {
//		expectedID = int32(f)
//	}
//
//	if expectedID != nil {
//		assert.NotNil(t, res)
//		assert.Equal(t, expectedID, res.InsertedID)
//	}
//}
//
//func verifyInsertManyResult(t *mtest.T, res *mongo.InsertManyResult, result json.RawMessage) {
//	expectedBytes, err := result.MarshalJSON()
//	assert.Nil(t, err)
//
//	var expected struct{ InsertedIds map[string]interface{} }
//	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
//	assert.Nil(t, err)
//
//	if expected.InsertedIds != nil {
//		assert.NotNil(t, res)
//		replaceFloatsWithInts(expected.InsertedIds)
//
//		for _, val := range expected.InsertedIds {
//			assert.Contains(t, res.InsertedIDs, val)
//		}
//	}
//}
//
//func verifyCursorResult(t *mtest.T, cur *mongo.Cursor, result json.RawMessage) {
//	for _, expected := range docSliceFromRaw(t, result) {
//		assert.NotNil(t, cur)
//		assert.True(t, cur.Next(context.Background()))
//
//		var actual bsonx.Doc
//		assert.Nil(t, cur.Decode(&actual))
//
//		compareDocs(t, expected, actual)
//	}
//
//	assert.False(t, cur.Next(ctx))
//	assert.Nil(t, cur.Err())
//}
//
//func verifySingleResult(t *mtest.T, res *mongo.SingleResult, result json.RawMessage) {
//	jsonBytes, err := result.MarshalJSON()
//	assert.Nil(t, err)
//
//	var actual bsonx.Doc
//	err = res.Decode(&actual)
//	if err == ErrNoDocuments {
//		var expected map[string]interface{}
//		err := json.NewDecoder(bytes.NewBuffer(jsonBytes)).Decode(&expected)
//		assert.Nil(t, err)
//
//		assert.Nil(t, expected)
//		return
//	}
//
//	assert.Nil(t, err)
//
//	doc := bsonx.Doc{}
//	err = bson.UnmarshalExtJSON(jsonBytes, true, &doc)
//	assert.Nil(t, err)
//
//	assert.True(t, doc.Equal(actual))
//}
//
//func verifyDistinctResult(t *mtest.T, res []interface{}, result json.RawMessage) {
//	resultBytes, err := result.MarshalJSON()
//	assert.Nil(t, err)
//
//	var expected []interface{}
//	assert.Nil(t, json.NewDecoder(bytes.NewBuffer(resultBytes)).Decode(&expected))
//
//	assert.Equal(t, len(expected), len(res))
//
//	for i := range expected {
//		expectedElem := expected[i]
//		actualElem := res[i]
//
//		iExpected := testhelpers.GetIntFromInterface(expectedElem)
//		iActual := testhelpers.GetIntFromInterface(actualElem)
//
//		assert.Equal(t, iExpected == nil, iActual == nil)
//		if iExpected != nil {
//			assert.Equal(t, *iExpected, *iActual)
//			continue
//		}
//
//		assert.Equal(t, expected[i], res[i])
//	}
//}
//
//func verifyDeleteResult(t *mtest.T, res *mongo.DeleteResult, result json.RawMessage) {
//	expectedBytes, err := result.MarshalJSON()
//	assert.Nil(t, err)
//
//	var expected DeleteResult
//	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
//	assert.Nil(t, err)
//
//	assert.Equal(t, expected.DeletedCount, res.DeletedCount)
//}
//
//func verifyUpdateResult(t *mtest.T, res *mongo.UpdateResult, result json.RawMessage) {
//	expectedBytes, err := result.MarshalJSON()
//	assert.Nil(t, err)
//
//	var expected struct {
//		MatchedCount  int64
//		ModifiedCount int64
//		UpsertedCount int64
//	}
//	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
//
//	assert.Equal(t, expected.MatchedCount, res.MatchedCount)
//	assert.Equal(t, expected.ModifiedCount, res.ModifiedCount)
//
//	actualUpsertedCount := int64(0)
//	if res.UpsertedID != nil {
//		actualUpsertedCount = 1
//	}
//
//	assert.Equal(t, expected.UpsertedCount, actualUpsertedCount)
//}
//
//func verifyRunCommandResult(t *mtest.T, res bson.Raw, result json.RawMessage) {
//	if len(result) == 0 {
//		return
//	}
//	jsonBytes, err := result.MarshalJSON()
//	assert.Nil(t, err, "Error marshaling JSON: %v", err)
//
//	expected := bsonx.Doc{}
//	err = bson.UnmarshalExtJSON(jsonBytes, true, &expected)
//	assert.Nil(t, err, "Error unmarshaling JSON: %v", err)
//
//	assert.NotNil(t, res)
//	actual, err := bsonx.ReadDoc(res)
//	assert.Nil(t, err, "Error reading result: %v", err)
//
//	// All runcommand results in tests are for key "n" only
//	compareElements(t, expected.LookupElement("n"), actual.LookupElement("n"))
//}
//
//func verifyCollectionContents(t *mtest.T, coll *mongo.Collection, result json.RawMessage) {
//	cursor, err := coll.Find(context.Background(), bsonx.Doc{})
//	assert.Nil(t, err, "Error performing find: %v", err)
//
//	verifyCursorResult(t, cursor, result)
//}
//

