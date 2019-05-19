package integration

import (
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestMain(m *testing.M) {
	if err := mtest.Setup(); err != nil {
		os.Exit(1)
	}
	defer os.Exit(m.Run())
	if err := mtest.Teardown(); err != nil {
		os.Exit(1)
	}
}
