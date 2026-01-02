package pds

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"log/slog"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/plc"
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

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	return &server{
		log:        slog.Default(),
		tracer:     otel.Tracer("test"),
		db:         testDB,
		directory:  &dir,
		signingKey: signingKey,
		serviceDID: "did:plc:test-service-12345",
		plc:        &plc.MockClient{},
	}
}
