package integration

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"math"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// findJSONFilesInDir finds the JSON files in a directory.
func findJSONFilesInDir(t *testing.T, dir string) []string {
	t.Helper()
	files := make([]string, 0)

	entries, err := ioutil.ReadDir(dir)
	assert.Nil(t, err, "unable to read json file: %v", err)

	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".json" {
			continue
		}

		files = append(files, entry.Name())
	}

	return files
}

func killSessions(mt *mtest.T) {
	mt.Helper()
	err := mt.Client.Database("admin").RunCommand(mtest.Background, bson.D{
		{"killAllSessions", bson.A{}},
	}, options.RunCmd().SetReadPreference(mtest.PrimaryRp)).Err()
	assert.Nil(mt, err, "unable to killAllSessions: %v", err)
}

func distinctWorkaround(mt *mtest.T, db, coll string) {
	mt.Helper()
	opts := options.Client().ApplyURI(mt.ConnString())
	hosts := opts.Hosts
	for _, host := range hosts {
		client, err := mongo.Connect(mtest.Background, opts.SetHosts([]string{host}))
		assert.Nil(mt, err, "error connecting sharded client: %v", err)
		defer func() { _ = client.Disconnect(mtest.Background) }()
		_, err = client.Database(db).Collection(coll).Distinct(mtest.Background, "x", bson.D{})
		assert.Nil(mt, err, "error running Distinct: %v", err)
	}
}

func createClientOptions(optsMap map[string]interface{}) *options.ClientOptions {
	opts := options.Client()
	for name, opt := range optsMap {
		switch name {
		case "retryWrites":
			opts.SetRetryWrites(opt.(bool))
		case "w":
			var wc *writeconcern.WriteConcern
			switch opt.(type) {
			case float64:
				wc = writeconcern.New(writeconcern.W(int(opt.(float64))))
			case string:
				wc = writeconcern.New(writeconcern.WMajority())
			}
			opts.SetWriteConcern(wc)
		case "readConcernLevel":
			rc := readconcern.New(readconcern.Level(opt.(string)))
			opts.SetReadConcern(rc)
		case "readPreference":
			rp := readPrefFromString(opt.(string))
			opts.SetReadPreference(rp)
		}
	}
	return opts
}

func createSessionOptions(opts map[string]interface{}) *options.SessionOptions {
	sessOpts := options.Session()
	for name, opt := range opts {
		switch name {
		case "causalConsistency":
			sessOpts = sessOpts.SetCausalConsistency(opt.(bool))
		case "defaultTransactionOptions":
			transOpts := opt.(map[string]interface{})
			if transOpts["readConcern"] != nil {
				sessOpts = sessOpts.SetDefaultReadConcern(getReadConcern(transOpts["readConcern"]))
			}
			if transOpts["writeConcern"] != nil {
				sessOpts = sessOpts.SetDefaultWriteConcern(getWriteConcern(transOpts["writeConcern"]))
			}
			if transOpts["readPreference"] != nil {
				sessOpts = sessOpts.SetDefaultReadPreference(getReadPref(transOpts["readPreference"]))
			}
			if transOpts["maxCommitTimeMS"] != nil {
				sessOpts = sessOpts.SetDefaultMaxCommitTime(getMaxCommitTime(transOpts["maxCommitTimeMS"]))
			}
		}
	}

	return sessOpts
}

func createCollectionOptions(opts map[string]interface{}) *options.CollectionOptions {
	collOpts := options.Collection()
	for name, opt := range opts {
		switch name {
		case "readConcern":
			collOpts = collOpts.SetReadConcern(getReadConcern(opt))
		case "writeConcern":
			collOpts = collOpts.SetWriteConcern(getWriteConcern(opt))
		case "readPreference":
			collOpts = collOpts.SetReadPreference(readPrefFromString(opt.(map[string]interface{})["mode"].(string)))
		}
	}
	return collOpts
}

func createTransactionOptions(opts map[string]interface{}) *options.TransactionOptions {
	transOpts := options.Transaction()
	for name, opt := range opts {
		switch name {
		case "writeConcern":
			transOpts = transOpts.SetWriteConcern(getWriteConcern(opt))
		case "readPreference":
			transOpts = transOpts.SetReadPreference(getReadPref(opt))
		case "readConcern":
			transOpts = transOpts.SetReadConcern(getReadConcern(opt))
		case "maxCommitTimeMS":
			transOpts = transOpts.SetMaxCommitTime(getMaxCommitTime(opt))
		}
	}
	return transOpts
}

func readPrefFromString(s string) *readpref.ReadPref {
	switch strings.ToLower(s) {
	case "primary":
		return readpref.Primary()
	case "primarypreferred":
		return readpref.PrimaryPreferred()
	case "secondary":
		return readpref.Secondary()
	case "secondarypreferred":
		return readpref.SecondaryPreferred()
	case "nearest":
		return readpref.Nearest()
	}
	return readpref.Primary()
}

func getReadConcern(opt interface{}) *readconcern.ReadConcern {
	return readconcern.New(readconcern.Level(opt.(map[string]interface{})["level"].(string)))
}

func getWriteConcern(opt interface{}) *writeconcern.WriteConcern {
	if w, ok := opt.(map[string]interface{}); ok {
		var newTimeout time.Duration
		if conv, ok := w["wtimeout"].(float64); ok {
			newTimeout = time.Duration(int(conv)) * time.Millisecond
		}
		var newJ bool
		if conv, ok := w["j"].(bool); ok {
			newJ = conv
		}
		if conv, ok := w["w"].(string); ok && conv == "majority" {
			return writeconcern.New(writeconcern.WMajority(), writeconcern.J(newJ), writeconcern.WTimeout(newTimeout))
		} else if conv, ok := w["w"].(float64); ok {
			return writeconcern.New(writeconcern.W(int(conv)), writeconcern.J(newJ), writeconcern.WTimeout(newTimeout))
		}
	}
	return nil
}

func getReadPref(opt interface{}) *readpref.ReadPref {
	if conv, ok := opt.(map[string]interface{}); ok {
		return readPrefFromString(conv["mode"].(string))
	}
	return nil
}

func getMaxCommitTime(opt interface{}) *time.Duration {
	if max, ok := opt.(float64); ok {
		res := time.Duration(max) * time.Millisecond
		return &res
	}
	return nil
}

func docSliceToInterfaceSlice(docs []bson.Raw) []interface{} {
	out := make([]interface{}, 0, len(docs))

	for _, doc := range docs {
		out = append(out, doc)
	}

	return out
}

func docSliceFromRaw(mt *mtest.T, raw json.RawMessage) []bson.Raw {
	mt.Helper()
	jsonBytes, err := raw.MarshalJSON()
	assert.Nil(mt, err, "unable to marshal data: %v", err)

	var arr []bson.Raw
	if err = bson.UnmarshalExtJSON(jsonBytes, true, &arr); err != nil {
		mt.Fatalf("bson.UnmarshalExtJSON error: %v", err)
	}
	return arr
}

// convert operation arguments from raw message into map
func createArgMap(mt *mtest.T, args json.RawMessage) map[string]interface{} {
	mt.Helper()
	if args == nil {
		return nil
	}
	var argmap map[string]interface{}
	err := json.Unmarshal(args, &argmap)
	assert.Nil(mt, err, "json.Unmarshal error: %v", err)
	return argmap
}

func resultHasError(mt *mtest.T, result json.RawMessage) bool {
	if result == nil {
		return false
	}
	res := getErrorFromResult(mt, result)
	if res == nil {
		return false
	}
	return res.ErrorLabelsOmit != nil ||
		res.ErrorLabelsContain != nil ||
		res.ErrorCodeName != "" ||
		res.ErrorContains != ""
}

func getErrorFromResult(mt *mtest.T, result json.RawMessage) *txnError {
	expectedBytes, err := result.MarshalJSON()
	assert.Nil(mt, err, "result.MarshalJSON error: %v", err)

	var expected txnError
	err = json.NewDecoder(bytes.NewBuffer(expectedBytes)).Decode(&expected)
	if err != nil {
		return nil
	}
	return &expected
}

func collationFromMap(m map[string]interface{}) *options.Collation {
	var collation options.Collation

	if locale, found := m["locale"]; found {
		collation.Locale = locale.(string)
	}

	if caseLevel, found := m["caseLevel"]; found {
		collation.CaseLevel = caseLevel.(bool)
	}

	if caseFirst, found := m["caseFirst"]; found {
		collation.CaseFirst = caseFirst.(string)
	}

	if strength, found := m["strength"]; found {
		collation.Strength = int(strength.(float64))
	}

	if numericOrdering, found := m["numericOrdering"]; found {
		collation.NumericOrdering = numericOrdering.(bool)
	}

	if alternate, found := m["alternate"]; found {
		collation.Alternate = alternate.(string)
	}

	if maxVariable, found := m["maxVariable"]; found {
		collation.MaxVariable = maxVariable.(string)
	}

	if normalization, found := m["normalization"]; found {
		collation.Normalization = normalization.(bool)
	}

	if backwards, found := m["backwards"]; found {
		collation.Backwards = backwards.(bool)
	}

	return &collation
}

func compareValues(mt *mtest.T, expected, actual bson.RawValue) {
	mt.Helper()
	switch expected.Type {
	case bson.TypeInt64:
		e := expected.Int64()

		switch actual.Type {
		case bson.TypeInt32:
			a := actual.Int32()
			assert.Equal(mt, e, int64(a), "value mismatch; expected %v, got %v", e, a)
		case bson.TypeInt64:
			a := actual.Int64()
			assert.Equal(mt, e, a, "value mismatch; expected %v, got %v", e, a)
		case bson.TypeDouble:
			a := actual.Double()
			assert.Equal(mt, e, int64(a), "value mismatch; expected %v, got %v", e, a)
		}
	case bson.TypeInt32:
		e := expected.Int32()

		switch actual.Type {
		case bson.TypeInt32:
			a := actual.Int32()
			assert.Equal(mt, e, a, "value mismatch; expected %v, got %v", e, a)
		case bson.TypeInt64:
			a := actual.Int64()
			assert.Equal(mt, e, int32(a), "value mismatch; expected %v, got %v", e, a)
		case bson.TypeDouble:
			a := actual.Double()
			assert.Equal(mt, e, int32(a), "value mismatch; expected %v, got %v", e, a)
		}
	case bson.TypeEmbeddedDocument:
		e := expected.Document()
		a := actual.Document()
		compareDocs(mt, e, a)
	case bson.TypeArray:
		e := expected.Array()
		a := actual.Array()
		compareDocs(mt, e, a)
	default:
		require.Equal(mt, expected.Value, actual.Value,
			"value mismatch; expected %v, got %v", expected.Value, actual.Value)
	}
}

func compareDocs(mt *mtest.T, expected, actual bson.Raw) {
	mt.Helper()
	eElems, err := expected.Elements()
	assert.Nil(mt, err, "error getting expected elements: %v", err)
	aElems, err := actual.Elements()
	assert.Nil(mt, err, "error getting actual elements: %v", err)
	assert.Equal(mt, len(eElems), len(aElems), "elements length mismatch; expected %d, got %d", len(eElems), len(aElems))

	for _, e := range eElems {
		aVal, err := actual.LookupErr(e.Key())
		assert.Nil(mt, err, "key %s not found in result", e.Key())

		eVal := e.Value()
		if doc, ok := eVal.DocumentOK(); ok {
			// nested doc
			compareDocs(mt, doc, aVal.Document())
			continue
		}

		compareValues(mt, eVal, aVal)
	}
}

func verifyError(mt *mtest.T, e error, result json.RawMessage) {
	mt.Helper()
	expected := getErrorFromResult(mt, result)
	if expected == nil {
		return
	}

	if cerr, ok := e.(mongo.CommandError); ok {
		if expected.ErrorCodeName != "" {
			assert.Equal(mt, expected.ErrorCodeName, cerr.Name,
				"code name mismatch; expected %v, got %v", expected.ErrorCodeName, cerr.Name)
		}
		if expected.ErrorContains != "" {
			msg := strings.ToLower(cerr.Message)
			substr := strings.ToLower(expected.ErrorContains)
			contains := strings.Contains(msg, substr)
			assert.True(mt, contains, "expected error '%v' to contain substring '%v'", cerr.Message, substr)
		}
		if expected.ErrorLabelsContain != nil {
			for _, label := range expected.ErrorLabelsContain {
				assert.True(mt, cerr.HasErrorLabel(label), "expected error %v to contain label %v", cerr, label)
			}
		}
		if expected.ErrorLabelsOmit != nil {
			for _, label := range expected.ErrorLabelsOmit {
				assert.False(mt, cerr.HasErrorLabel(label), "expected error %v to not contain label %v", cerr, label)
			}
		}
		return
	}
	// not CommandError
	assert.Equal(mt, expected.ErrorCodeName, "", "expected empty ErrorCodeName but got %v", expected.ErrorCodeName)
	assert.Equal(mt, len(expected.ErrorLabelsContain), 0,
		"expected empty ErrorLabelsContain slice but got %v", expected.ErrorLabelsContain)
	// ErrorLabelsOmit can contain anything, since they are all omitted for e not type CommandError so we do not check
	// that here
	if expected.ErrorContains != "" {
		errStr := strings.ToLower(e.Error())
		substr := strings.ToLower(expected.ErrorContains)
		contains := strings.Contains(errStr, substr)
		assert.True(mt, contains, "expected error '%v' to contain substring '%v'", errStr, substr)
	}
}

func replaceFloatsWithInts(m map[string]interface{}) {
	for key, val := range m {
		if f, ok := val.(float64); ok && f == math.Floor(f) {
			m[key] = int32(f)
			continue
		}

		if innerM, ok := val.(map[string]interface{}); ok {
			replaceFloatsWithInts(innerM)
			m[key] = innerM
		}
	}
}

func getIntFromInterface(i interface{}) *int64 {
	var out int64

	switch v := i.(type) {
	case int:
		out = int64(v)
	case int32:
		out = int64(v)
	case int64:
		out = v
	case float32:
		f := float64(v)
		if math.Floor(f) != f || f > float64(math.MaxInt64) {
			break
		}

		out = int64(f)

	case float64:
		if math.Floor(v) != v || v > float64(math.MaxInt64) {
			break
		}

		out = int64(v)
	default:
		return nil
	}

	return &out
}

func checkExpectations(mt *mtest.T, expectations []*expectation, id0 bsonx.Doc, id1 bsonx.Doc) {
	mt.Helper()
	started := mt.GetAllStartedEvents()

	for i, expectation := range expectations {
		expected := expectation.CommandStartedEvent
		if i == len(started) {
			mt.Fatalf("expected event for %s", expectation.CommandStartedEvent.CommandName)
		}

		evt := started[i]
		if expected.CommandName != "" {
			assert.Equal(mt, expected.CommandName, evt.CommandName,
				"cmd name mismatch; expected %s, got %s", expected.CommandName, evt.CommandName)
		}
		if expected.DatabaseName != "" {
			assert.Equal(mt, expected.DatabaseName, evt.DatabaseName,
				"db name mismatch; expected %s, got %s", expected.DatabaseName, evt.DatabaseName)
		}

		jsonBytes, err := expected.Command.MarshalJSON()
		assert.Nil(mt, err, "MarshalJSON error for command: %v", err)
		var expectedCmd bson.Raw
		err = bson.UnmarshalExtJSON(jsonBytes, true, &expectedCmd)
		assert.Nil(mt, err, "UnmarshalExtJSON error for command: %v", err)

		eElems, err := expectedCmd.Elements()
		assert.Nil(mt, err, "error getting expected elements: %v", err)

		for _, elem := range eElems {
			key := elem.Key()
			val := elem.Value()

			actualVal := evt.Command.Lookup(key)

			// Keys that may be nil
			if val.Type == bson.TypeNull {
				assert.Equal(mt, bson.RawValue{}, actualVal, "expected value for key %s to be nil but got %v", key, actualVal)
				continue
			}
			if key == "ordered" {
				// TODO: some tests specify that "ordered" must be a key in the event but ordered isn't a valid option for some of these cases (e.g. insertOne)
				continue
			}

			// keys that should not be nil
			assert.NotEqual(mt, bson.TypeNull, actualVal.Type, "expected value %v for key %s but got nil", val, key)
			err = actualVal.Validate()
			assert.Nil(mt, err, "error validating value: %v for key %s", err, key)

			switch key {
			case "lsid":
				// TODO lsid
			case "getMore":
				expectedID := val.Int64()
				// ignore placeholder cursor ID (42)
				if expectedID != 42 {
					actualID := actualVal.Int64()
					assert.Equal(mt, expectedID, actualID, "cursor ID mismatch; expected %v, got %v", expectedID, actualID)
				}
			case "readConcern":
				expectedRc := val.Document()
				actualRc := actualVal.Document()
				eClusterTime := expectedRc.Lookup("afterClusterTime")
				eLevel := expectedRc.Lookup("level")

				if eClusterTime.Type != bson.TypeNull {
					aClusterTime := actualRc.Lookup("afterClusterTime")
					// ignore placeholder cluster time (42)
					ctInt32, ok := eClusterTime.Int32OK()
					if ok && ctInt32 == 42 {
						continue
					}

					assert.Equal(mt, eClusterTime, aClusterTime,
						"cluster time mismatch; expected %v, got %v", eClusterTime, aClusterTime)
				}
				if eLevel.Type != bson.TypeNull {
					aLevel := actualRc.Lookup("level")
					assert.Equal(mt, eLevel, aLevel, "level mismatch; expected %v, got %v", eLevel, aLevel)
				}
			default:
				compareValues(mt, val, actualVal)
			}
		}
	}
}
