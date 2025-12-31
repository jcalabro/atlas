package pds

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

var (
	setupOnce sync.Once
	testDB    *foundation.DB
)

func testServer(t *testing.T) *server {
	tracer := otel.Tracer("test")

	var err error
	setupOnce.Do(func() {
		testDB, err = foundation.New(tracer, foundation.Config{
			ClusterFile: "../../foundation.cluster",
			APIVersion:  730,
		})
	})
	require.NoError(t, err)
	require.NotNil(t, testDB)

	dir := identity.NewMockDirectory()

	return &server{
		log:       slog.Default(),
		tracer:    otel.Tracer("test"),
		db:        testDB,
		directory: &dir,
	}
}
