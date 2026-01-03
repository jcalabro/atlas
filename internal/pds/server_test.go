package pds

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"log/slog"
	"net/http"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/plc"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	setupOnce sync.Once
	testDB    *foundation.DB
)

const testPDSHost = "dev.atlaspds.dev"

func testServer(t *testing.T) *server {
	t.Helper()

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
		log:    slog.Default(),
		tracer: otel.Tracer("test"),

		hosts: map[string]*loadedHostConfig{
			testPDSHost: {
				hostname:       testPDSHost,
				signingKey:     signingKey,
				serviceDID:     "did:web:dev.atlaspds.dev",
				userDomains:    []string{".dev.atlaspds.dev"},
				contactEmail:   "webmaster@dev.atlaspds.dev",
				privacyPolicy:  "https://dev.atlaspds.dev/privacy",
				termsOfService: "https://dev.atlaspds.dev/tos",
			},
		},

		db: testDB,

		directory: &dir,
		plc:       &plc.MockClient{},
	}
}

// helper to create an authenticated actor
func setupTestActor(t *testing.T, srv *server, did, email, handle string) (*types.Actor, *Session) {
	t.Helper()

	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	pwHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
	require.NoError(t, err)

	actor := &types.Actor{
		Did:           did,
		Email:         email,
		Handle:        handle,
		PdsHost:       testPDSHost,
		CreatedAt:     timestamppb.Now(),
		PasswordHash:  pwHash,
		SigningKey:    []byte("test-signing-key"),
		RotationKeys:  [][]byte{[]byte("test-rotation-key")},
		RefreshTokens: []*types.RefreshToken{},
		Active:        true,
	}

	err = srv.db.SaveActor(ctx, actor)
	require.NoError(t, err)

	session, err := srv.createSession(ctx, actor)
	require.NoError(t, err)

	return actor, session
}

func addAuthContext(t *testing.T, ctx context.Context, srv *server, req *http.Request, actor *types.Actor, accessToken string) *http.Request {
	t.Helper()

	req.Header.Set("Authorization", "Bearer "+accessToken)
	ctx = context.WithValue(ctx, hostContextKey{}, srv.hosts[testPDSHost])
	ctx = context.WithValue(ctx, actorContextKey{}, actor)
	return req.WithContext(ctx)
}
