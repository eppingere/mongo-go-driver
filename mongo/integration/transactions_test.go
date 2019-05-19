package integration
//
//import (
//	"testing"
//
//	"go.mongodb.org/mongo-driver/bson"
//	"go.mongodb.org/mongo-driver/internal/testutil/assert"
//	"go.mongodb.org/mongo-driver/mongo"
//	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
//)
//
//const transactionTestsDir = "../../data/transactions"
//
//type transTestFile struct {
//	RunOn          []*mtest.RunOnBlock `json:"runOn"`
//	DatabaseName   string              `json:"database_name"`
//	CollectionName string              `json:"collection_name"`
//	Data           json.RawMessage     `json:"data"`
//	Tests          []*transTestCase    `json:"tests"`
//}
//
//type transTestCase struct {
//	Description         string                 `json:"description"`
//	SkipReason          string                 `json:"skipReason"`
//	FailPoint           *mtest.FailPoint       `json:"failPoint"`
//	ClientOptions       map[string]interface{} `json:"clientOptions"`
//	SessionOptions      map[string]interface{} `json:"sessionOptions"`
//	Operations          []*transOperation      `json:"operations"`
//	Outcome             *transOutcome          `json:"outcome"`
//	Expectations        []*expectation         `json:"expectations"`
//	UseMultipleMongoses bool                   `json:"useMultipleMongoses"`
//}
//
//type transOperation struct {
//	Name              string                 `json:"name"`
//	Object            string                 `json:"object"`
//	CollectionOptions map[string]interface{} `json:"collectionOptions"`
//	Result            json.RawMessage        `json:"result"`
//	Arguments         json.RawMessage        `json:"arguments"`
//	ArgMap            map[string]interface{}
//	Error             bool `json:"error"`
//}
//
//type transOutcome struct {
//	Collection struct {
//		Data json.RawMessage `json:"data"`
//	} `json:"collection"`
//}
//
//type expectation struct {
//	CommandStartedEvent struct {
//		CommandName  string          `json:"command_name"`
//		DatabaseName string          `json:"database_name"`
//		Command      json.RawMessage `json:"command"`
//	} `json:"command_started_event"`
//}
//
//type transError struct {
//	ErrorContains      string   `bson:"errorContains"`
//	ErrorCodeName      string   `bson:"errorCodeName"`
//	ErrorLabelsContain []string `bson:"errorLabelsContain"`
//	ErrorLabelsOmit    []string `bson:"errorLabelsOmit"`
//}
//
//func TestTransactionsSpec(t *testing.T) {
//	for _, file := range testhelpers.FindJSONFilesInDir(t, transactionTestsDir) {
//		content, err := ioutil.ReadFile(filepath)
//		assert.Nil(t, err, "Error reading file: %v", err)
//
//		var testfile transTestFile
//		err = json.Unmarshal(content, &testfile)
//		assert.Nil(t, err, "Error unmarshaling file: %v", err)
//
//		opts := mtest.NewOptions().Constraints(testfile.RunOn)
//		if testfile.UseMultipleMongoses {
//			opts = opts.ClientType(mtest.Pinned)
//		}
//		mt := mtest.New(opts)
//		for _, test := range testfile.Tests {
//			runTransactionsTestCase(mt, test, testfile)
//		}
//	}
//}
//
//func runTransactionsTestCase(mt *mtest.T, test *transTestCase, testfile transTestFile) {
//	mt.Run(test.Description, func(t *mtest.T) {
//		if len(test.SkipReason) > 0 {
//			mt.Skip(test.SkipReason)
//		}
//
//
//	})
//}
