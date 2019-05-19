package mtest

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TopologyKind describes the topology that a test is run on.
type TopologyKind string

// These constants specify valid values for TopologyKind
const (
	ReplicaSet TopologyKind = "replicaset"
	Sharded = "sharded"
	Single = "single"
)

// ClientType specifies the type of Client that should be created for a test.
type ClientType int

// These constants specify valid values for ClientType
const (
	// Default specifies a client to the connection string in the MONGODB_URI env variable with command monitoring
	// enabled.
	Default ClientType = iota
	// Pinned specifies a client that is pinned to a single mongos in a sharded cluster.
	Pinned
	// Mock specifies a client that communicates with a mock deployment.
	Mock
)

// RunOnBlock describes a constraint for a test.
type RunOnBlock struct {
	MinServerVersion string         `json:"minServerVersion"`
	MaxServerVersion string         `json:"maxServerVersion"`
	MinWireVersion   int32          `json:"minWireVersion"`
	MaxWireVersion   int32          `json:"maxWireVersion"`
	Topology         []TopologyKind `json:"topology"`
}

// optionFunc is a function type that configures a T instance.
type optionFunc func(*T)

// Options is the type used to configure a new T instance.
type Options struct {
	optFuncs []optionFunc
}

// NewOptions creates an empty Options instance.
func NewOptions() *Options {
	return &Options{}
}

// CollectionOptions
func (op *Options) CollectionOptions(opts *options.CollectionOptions) *Options {
	op.optFuncs = append(op.optFuncs, func(t *T) {
		t.collOpts = opts
	})
	return op
}

// ClientOptions
func (op *Options) ClientOptions(opts *options.ClientOptions) *Options {
	op.optFuncs = append(op.optFuncs, func(t *T) {
		t.clientOpts = opts
	})
	return op
}

// CreateClient
func (op *Options) CreateClient(create bool) *Options {
	op.optFuncs = append(op.optFuncs, func(t *T) {
		t.createClient = &create
	})
	return op
}

// ClientType specifies the type of client that should be created for a test. This option will be propagated to all
// sub-tests.
func (op *Options) ClientType(ct ClientType) *Options {
	op.optFuncs = append(op.optFuncs, func(t *T) {
		t.clientType = ct
	})
	return op
}

// MockResponses specifies the responses returned by a mock deployment. This should only be used if the current test
// is being run with MockDeployment(true). Responses can also be added after a sub-test has already been created.
func (op *Options) MockResponses(responses ...bson.D) *Options {
	op.optFuncs = append(op.optFuncs, func(t *T) {
		t.mockResponses = responses
	})
	return op
}

// Constraints specifies environmental constraints for a test to run. If a test's environment meets at least one of the
// given constraints, it will be run. Otherwise, it will be skipped.
func (op *Options) Constraints(constraints ...RunOnBlock) *Options {
	op.optFuncs = append(op.optFuncs, func(t *T) {
		t.constraints = append(t.constraints, constraints...)
	})
	return op
}
