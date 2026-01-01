package foundation

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

var (
	setupOnce sync.Once

	// Should be retrieved via `testDB(t)`; don't use this directly
	testingDB *DB
)

func testDB(t *testing.T) *DB {
	tracer := otel.Tracer("test")

	var err error
	setupOnce.Do(func() {
		testingDB, err = New(tracer, Config{
			ClusterFile: "../../foundation.cluster",
			APIVersion:  730,
		})
	})
	require.NoError(t, err)
	require.NotNil(t, testingDB)

	return testingDB
}
