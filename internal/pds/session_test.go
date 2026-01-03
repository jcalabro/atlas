package pds

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCreateSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	// create a context with host config for session creation
	hostCtx := context.WithValue(ctx, hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("creates valid access and refresh tokens", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)
		require.NotNil(t, session)

		require.NotEmpty(t, session.AccessToken)
		require.NotEmpty(t, session.RefreshToken)

		accessToken, err := jwt.Parse(session.AccessToken, func(token *jwt.Token) (any, error) {
			return &signingKey.PublicKey, nil
		})
		require.NoError(t, err)
		require.True(t, accessToken.Valid)

		accessClaims, ok := accessToken.Claims.(jwt.MapClaims)
		require.True(t, ok)
		require.Equal(t, "com.atproto.access", accessClaims["scope"])
		require.Equal(t, "did:plc:test-service-12345", accessClaims["aud"])
		require.Equal(t, "did:plc:testuser123", accessClaims["sub"])
		require.NotEmpty(t, accessClaims["jti"])

		refreshToken, err := jwt.Parse(session.RefreshToken, func(token *jwt.Token) (any, error) {
			return &signingKey.PublicKey, nil
		})
		require.NoError(t, err)
		require.True(t, refreshToken.Valid)

		refreshClaims, ok := refreshToken.Claims.(jwt.MapClaims)
		require.True(t, ok)
		require.Equal(t, "com.atproto.refresh", refreshClaims["scope"])
		require.Equal(t, "did:plc:test-service-12345", refreshClaims["aud"])
		require.Equal(t, "did:plc:testuser123", refreshClaims["sub"])
		require.NotEmpty(t, refreshClaims["jti"])

		require.Equal(t, accessClaims["jti"], refreshClaims["jti"])
	})

	t.Run("saves refresh token to actor", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser456",
			Email:         "testsaverefresh@example.com",
			Handle:        "testsaverefresh.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		retrievedActor, err := srv.db.GetActorByEmail(ctx, testPDSHost, actor.Email)
		require.NoError(t, err)
		require.NotNil(t, retrievedActor)

		require.Len(t, retrievedActor.RefreshTokens, 1)
		require.Equal(t, session.RefreshToken, retrievedActor.RefreshTokens[0].Token)
		require.NotNil(t, retrievedActor.RefreshTokens[0].CreatedAt)
		require.NotNil(t, retrievedActor.RefreshTokens[0].ExpiresAt)

		expiresAt := retrievedActor.RefreshTokens[0].ExpiresAt.AsTime()
		expectedExpiry := time.Now().Add(refreshTokenTTL)
		require.WithinDuration(t, expectedExpiry, expiresAt, 5*time.Second)
	})

	t.Run("supports multiple refresh tokens per actor", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser789",
			Email:         "testmultirefresh@example.com",
			Handle:        "testmultirefresh.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session1, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		retrievedActor, err := srv.db.GetActorByEmail(ctx, testPDSHost, actor.Email)
		require.NoError(t, err)
		require.Len(t, retrievedActor.RefreshTokens, 1)

		session2, err := srv.createSession(hostCtx, retrievedActor)
		require.NoError(t, err)

		retrievedActor, err = srv.db.GetActorByEmail(ctx, testPDSHost, actor.Email)
		require.NoError(t, err)
		require.Len(t, retrievedActor.RefreshTokens, 2)

		require.Equal(t, session1.RefreshToken, retrievedActor.RefreshTokens[0].Token)
		require.Equal(t, session2.RefreshToken, retrievedActor.RefreshTokens[1].Token)
		require.NotEqual(t, session1.RefreshToken, session2.RefreshToken)
		require.NotEqual(t, session1.AccessToken, session2.AccessToken)
	})

	t.Run("access token expires in 3 hours", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser101112",
			Email:         "test4@example.com",
			Handle:        "test4.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		accessToken, err := jwt.Parse(session.AccessToken, func(token *jwt.Token) (any, error) {
			return &signingKey.PublicKey, nil
		})
		require.NoError(t, err)

		accessClaims, ok := accessToken.Claims.(jwt.MapClaims)
		require.True(t, ok)

		exp, ok := accessClaims["exp"].(float64)
		require.True(t, ok)

		expTime := time.Unix(int64(exp), 0)
		expectedExpiry := time.Now().Add(accessTokenTTL)
		require.WithinDuration(t, expectedExpiry, expTime, 5*time.Second)
	})

	t.Run("refresh token expires in 7 days", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser131415",
			Email:         "test5@example.com",
			Handle:        "test5.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		refreshToken, err := jwt.Parse(session.RefreshToken, func(token *jwt.Token) (any, error) {
			return &signingKey.PublicKey, nil
		})
		require.NoError(t, err)

		refreshClaims, ok := refreshToken.Claims.(jwt.MapClaims)
		require.True(t, ok)

		exp, ok := refreshClaims["exp"].(float64)
		require.True(t, ok)

		expTime := time.Unix(int64(exp), 0)
		expectedExpiry := time.Now().Add(refreshTokenTTL)
		require.WithinDuration(t, expectedExpiry, expTime, 5*time.Second)
	})
}

func TestVerifyAccessToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	// create a context with host config for session creation
	hostCtx := context.WithValue(ctx, hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("verifies valid access token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		claims, err := srv.verifyAccessToken(hostCtx, session.AccessToken)
		require.NoError(t, err)
		require.NotNil(t, claims)

		require.Equal(t, "did:plc:testuser123", claims.DID)
		require.Equal(t, "com.atproto.access", claims.Scope)
		require.NotEmpty(t, claims.JTI)
	})

	t.Run("rejects refresh token when expecting access token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser456rejectrefresh",
			Email:         "testrejectrefresh@example.com",
			Handle:        "testrejectrefresh.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(hostCtx, session.RefreshToken)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid scope")
	})

	t.Run("rejects expired access token", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		expiredTime := now.Add(-1 * time.Hour)

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"sub":   "did:plc:testuser789",
			"iat":   expiredTime.UTC().Unix(),
			"exp":   expiredTime.UTC().Unix(),
			"jti":   "test-jti-123",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(hostCtx, accessString)
		require.Error(t, err)
		require.Contains(t, err.Error(), "token is expired")
	})

	t.Run("rejects token with wrong audience", func(t *testing.T) {
		t.Parallel()

		now := time.Now()

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   "did:plc:wrong-service",
			"sub":   "did:plc:testuser101112",
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-456",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(hostCtx, accessString)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid audience")
	})

	t.Run("rejects token signed with wrong key", func(t *testing.T) {
		t.Parallel()

		wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		now := time.Now()

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"sub":   "did:plc:testuser131415",
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-789",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(wrongKey)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(hostCtx, accessString)
		require.Error(t, err)
	})

	t.Run("rejects malformed token", func(t *testing.T) {
		t.Parallel()

		_, err := srv.verifyAccessToken(hostCtx, "not.a.valid.jwt")
		require.Error(t, err)
	})

	t.Run("rejects token with missing claims", func(t *testing.T) {
		t.Parallel()

		now := time.Now()

		// Missing sub claim
		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-abc",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(hostCtx, accessString)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing or invalid sub claim")
	})
}

func TestVerifyRefreshToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	// create a context with host config for session creation
	hostCtx := context.WithValue(ctx, hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("verifies valid refresh token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		claims, err := srv.verifyRefreshToken(hostCtx, session.RefreshToken)
		require.NoError(t, err)
		require.NotNil(t, claims)

		require.Equal(t, "did:plc:testuser123", claims.DID)
		require.Equal(t, "com.atproto.refresh", claims.Scope)
		require.NotEmpty(t, claims.JTI)
	})

	t.Run("rejects access token when expecting refresh token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser456rejectaccess",
			Email:         "testrejectaccess@example.com",
			Handle:        "testrejectaccess.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		_, err = srv.verifyRefreshToken(hostCtx, session.AccessToken)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid scope")
	})

	t.Run("rejects expired refresh token", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		expiredTime := now.Add(-8 * 24 * time.Hour)

		refreshClaims := jwt.MapClaims{
			"scope": "com.atproto.refresh",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"sub":   "did:plc:testuser789",
			"iat":   expiredTime.UTC().Unix(),
			"exp":   expiredTime.UTC().Unix(),
			"jti":   "test-jti-123",
		}

		refreshToken := jwt.NewWithClaims(jwt.SigningMethodES256, refreshClaims)
		refreshString, err := refreshToken.SignedString(signingKey)
		require.NoError(t, err)

		_, err = srv.verifyRefreshToken(hostCtx, refreshString)
		require.Error(t, err)
		require.Contains(t, err.Error(), "token is expired")
	})

	t.Run("access and refresh tokens have matching JTI", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser101112",
			Email:         "testjti@example.com",
			Handle:        "testjti.dev.atlaspds.net",
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		accessClaims, err := srv.verifyAccessToken(hostCtx, session.AccessToken)
		require.NoError(t, err)

		refreshClaims, err := srv.verifyRefreshToken(hostCtx, session.RefreshToken)
		require.NoError(t, err)

		require.Equal(t, accessClaims.JTI, refreshClaims.JTI)
		require.Equal(t, accessClaims.DID, refreshClaims.DID)
	})
}

func TestHandleCreateSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	setupTestActor := func(did, email, handle, password string) *types.Actor {
		pwHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            did,
			Email:          email,
			Handle:         handle,
			PdsHost:        testPDSHost,
			PasswordHash:   pwHash,
			EmailConfirmed: true,
			CreatedAt:      timestamppb.Now(),
			Active:         true,
			SigningKey:     []byte("signing_key"),
			RotationKeys:   [][]byte{[]byte("rotation_key")},
			RefreshTokens:  []*types.RefreshToken{{Token: "initial_token"}},
		}

		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		return actor
	}

	// helper to add host context to requests
	addHostContext := func(req *http.Request) *http.Request {
		ctx := context.WithValue(req.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		return req.WithContext(ctx)
	}

	t.Run("creates session with DID identifier", func(t *testing.T) {
		t.Parallel()

		actor := setupTestActor("did:plc:testuser1", "test1@example.com", "test1.dev.atlaspds.net", "password123")

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"did:plc:testuser1","password":"password123"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		require.NotEmpty(t, resp["accessJwt"])
		require.NotEmpty(t, resp["refreshJwt"])
		require.Equal(t, actor.Did, resp["did"])
		require.Equal(t, actor.Handle, resp["handle"])
		require.Equal(t, actor.Email, resp["email"])
		require.Equal(t, true, resp["emailConfirmed"])
		require.Equal(t, true, resp["active"])
	})

	t.Run("creates session with handle identifier", func(t *testing.T) {
		t.Parallel()

		actor := setupTestActor("did:plc:testuser2", "test2@example.com", "test2.dev.atlaspds.net", "password456")

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"test2.dev.atlaspds.net","password":"password456"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		require.Equal(t, actor.Did, resp["did"])
		require.Equal(t, actor.Handle, resp["handle"])
	})

	t.Run("creates session with email identifier", func(t *testing.T) {
		t.Parallel()

		actor := setupTestActor("did:plc:testuser3", "test3@example.com", "test3.dev.atlaspds.net", "password789")

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"test3@example.com","password":"password789"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		require.Equal(t, actor.Did, resp["did"])
		require.Equal(t, actor.Handle, resp["handle"])
		require.Equal(t, actor.Email, resp["email"])
	})

	t.Run("rejects invalid password", func(t *testing.T) {
		t.Parallel()

		setupTestActor("did:plc:testuser4", "test4@example.com", "test4.dev.atlaspds.net", "correctpassword")

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"test4@example.com","password":"wrongpassword"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("rejects non-existent user", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"nonexistent@example.com","password":"password"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("rejects missing identifier", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"password":"password"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("rejects missing password", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"test@example.com"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("includes deactivated status for inactive account", func(t *testing.T) {
		t.Parallel()

		pwHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            "did:plc:testuser5",
			Email:          "test5@example.com",
			Handle:         "test5.dev.atlaspds.net",
			PdsHost:        testPDSHost,
			PasswordHash:   pwHash,
			EmailConfirmed: true,
			CreatedAt:      timestamppb.Now(),
			Active:         false, // inactive account
			SigningKey:     []byte("signing_key"),
			RotationKeys:   [][]byte{[]byte("rotation_key")},
			RefreshTokens:  []*types.RefreshToken{{Token: "initial_token"}},
		}

		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"test5@example.com","password":"password"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		require.Equal(t, false, resp["active"])
		require.Equal(t, "deactivated", resp["status"])
	})
}

func TestHandleGetSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	// create a context with host config for session creation
	hostCtx := context.WithValue(ctx, hostContextKey{}, srv.hosts[testPDSHost])

	setupTestActor := func(did, email, handle string) (*types.Actor, *Session) {
		pwHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            did,
			Email:          email,
			Handle:         handle,
			PdsHost:        testPDSHost,
			PasswordHash:   pwHash,
			EmailConfirmed: true,
			CreatedAt:      timestamppb.Now(),
			Active:         true,
			SigningKey:     []byte("signing_key"),
			RotationKeys:   [][]byte{[]byte("rotation_key")},
			RefreshTokens:  []*types.RefreshToken{},
		}

		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		return actor, session
	}

	// helper to add host context to requests
	addHostContext := func(req *http.Request) *http.Request {
		ctx := context.WithValue(req.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		return req.WithContext(ctx)
	}

	t.Run("returns session info with valid access token", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor("did:plc:getsession1", "get1@example.com", "get1.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		require.Equal(t, actor.Did, resp["did"])
		require.Equal(t, actor.Handle, resp["handle"])
		require.Equal(t, actor.Email, resp["email"])
		require.Equal(t, true, resp["emailConfirmed"])
		require.Equal(t, true, resp["active"])
	})

	t.Run("returns deactivated status for inactive account", func(t *testing.T) {
		t.Parallel()

		pwHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            "did:plc:getsession2",
			Email:          "get2@example.com",
			Handle:         "get2.dev.atlaspds.net",
			PdsHost:        testPDSHost,
			PasswordHash:   pwHash,
			EmailConfirmed: false,
			CreatedAt:      timestamppb.Now(),
			Active:         false,
			SigningKey:     []byte("signing_key"),
			RotationKeys:   [][]byte{[]byte("rotation_key")},
			RefreshTokens:  []*types.RefreshToken{},
		}

		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		require.Equal(t, false, resp["active"])
		require.Equal(t, "deactivated", resp["status"])
	})

	t.Run("rejects request without authorization header", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects request with invalid token", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer invalid.token.here")
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects refresh token for getSession", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor("did:plc:getsession3", "get3@example.com", "get3.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.RefreshToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestHandleRefreshSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	// create a context with host config for session creation
	hostCtx := context.WithValue(ctx, hostContextKey{}, srv.hosts[testPDSHost])

	setupTestActor := func(did, email, handle string) (*types.Actor, *Session) {
		pwHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            did,
			Email:          email,
			Handle:         handle,
			PdsHost:        testPDSHost,
			PasswordHash:   pwHash,
			EmailConfirmed: true,
			CreatedAt:      timestamppb.Now(),
			Active:         true,
			SigningKey:     []byte("signing_key"),
			RotationKeys:   [][]byte{[]byte("rotation_key")},
			RefreshTokens:  []*types.RefreshToken{},
		}

		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		return actor, session
	}

	// helper to add host context to requests
	addHostContext := func(req *http.Request) *http.Request {
		ctx := context.WithValue(req.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		return req.WithContext(ctx)
	}

	t.Run("refreshes session with valid refresh token", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor("did:plc:refresh1", "refresh1@example.com", "refresh1.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.RefreshToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		require.NotEmpty(t, resp["accessJwt"])
		require.NotEmpty(t, resp["refreshJwt"])
		require.Equal(t, actor.Did, resp["did"])
		require.Equal(t, actor.Handle, resp["handle"])
		require.Equal(t, true, resp["active"])

		// verify old tokens are different from new tokens
		require.NotEqual(t, session.AccessToken, resp["accessJwt"])
		require.NotEqual(t, session.RefreshToken, resp["refreshJwt"])

		// verify old refresh token was removed
		updatedActor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)
		for _, rt := range updatedActor.RefreshTokens {
			require.NotEqual(t, session.RefreshToken, rt.Token)
		}
	})

	t.Run("rejects access token for refreshSession", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor("did:plc:refresh2", "refresh2@example.com", "refresh2.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects request without authorization header", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects expired refresh token", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		expiredTime := now.Add(-8 * 24 * time.Hour)

		refreshClaims := jwt.MapClaims{
			"scope": "com.atproto.refresh",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"sub":   "did:plc:refresh3",
			"iat":   expiredTime.UTC().Unix(),
			"exp":   expiredTime.UTC().Unix(),
			"jti":   "test-jti-123",
		}

		refreshToken := jwt.NewWithClaims(jwt.SigningMethodES256, refreshClaims)
		refreshString, err := refreshToken.SignedString(signingKey)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil)
		req.Header.Set("Authorization", "Bearer "+refreshString)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestHandleDeleteSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	// create a context with host config for session creation
	hostCtx := context.WithValue(ctx, hostContextKey{}, srv.hosts[testPDSHost])

	setupTestActor := func(did, email, handle string) (*types.Actor, *Session) {
		pwHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            did,
			Email:          email,
			Handle:         handle,
			PdsHost:        testPDSHost,
			PasswordHash:   pwHash,
			EmailConfirmed: true,
			CreatedAt:      timestamppb.Now(),
			Active:         true,
			SigningKey:     []byte("signing_key"),
			RotationKeys:   [][]byte{[]byte("rotation_key")},
			RefreshTokens:  []*types.RefreshToken{},
		}

		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		return actor, session
	}

	// helper to add host context to requests
	addHostContext := func(req *http.Request) *http.Request {
		ctx := context.WithValue(req.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		return req.WithContext(ctx)
	}

	t.Run("deletes session with valid access token", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor("did:plc:delete1", "delete1@example.com", "delete1.dev.atlaspds.net")

		// verify refresh token exists before deletion
		beforeActor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)
		require.Len(t, beforeActor.RefreshTokens, 1)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.deleteSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// verify refresh token was removed
		afterActor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)
		require.Len(t, afterActor.RefreshTokens, 0)
	})

	t.Run("preserves other sessions when deleting one", func(t *testing.T) {
		t.Parallel()

		actor, session1 := setupTestActor("did:plc:delete2", "delete2@example.com", "delete2.dev.atlaspds.net")

		// create a second session
		updatedActor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)
		session2, err := srv.createSession(hostCtx, updatedActor)
		require.NoError(t, err)

		// verify two refresh tokens exist
		beforeActor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)
		require.Len(t, beforeActor.RefreshTokens, 2)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.deleteSession", nil)
		req.Header.Set("Authorization", "Bearer "+session1.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// verify only the first session's refresh token was removed
		afterActor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)
		require.Len(t, afterActor.RefreshTokens, 1)
		require.Equal(t, session2.RefreshToken, afterActor.RefreshTokens[0].Token)
	})

	t.Run("rejects request without authorization header", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.deleteSession", nil)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects refresh token for deleteSession", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor("did:plc:delete3", "delete3@example.com", "delete3.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.deleteSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.RefreshToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects invalid token", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.deleteSession", nil)
		req.Header.Set("Authorization", "Bearer invalid.token.here")
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestAuthMiddleware(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	// create a context with host config for session creation
	hostCtx := context.WithValue(ctx, hostContextKey{}, srv.hosts[testPDSHost])

	setupTestActor := func(did, email, handle string) (*types.Actor, *Session) {
		pwHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            did,
			Email:          email,
			Handle:         handle,
			PdsHost:        testPDSHost,
			PasswordHash:   pwHash,
			EmailConfirmed: true,
			CreatedAt:      timestamppb.Now(),
			Active:         true,
			SigningKey:     []byte("signing_key"),
			RotationKeys:   [][]byte{[]byte("rotation_key")},
			RefreshTokens:  []*types.RefreshToken{},
		}

		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(hostCtx, actor)
		require.NoError(t, err)

		return actor, session
	}

	// helper to add host context to requests
	addHostContext := func(req *http.Request) *http.Request {
		ctx := context.WithValue(req.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		return req.WithContext(ctx)
	}

	t.Run("accepts valid access token for access endpoint", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor("did:plc:authmw1", "authmw1@example.com", "authmw1.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("accepts valid refresh token for refresh endpoint", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor("did:plc:authmw2", "authmw2@example.com", "authmw2.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.RefreshToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("rejects missing authorization header", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		require.Contains(t, resp["msg"], "missing authorization header")
	})

	t.Run("rejects malformed authorization header", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			header string
		}{
			{"no bearer prefix", "token123"},
			{"wrong prefix", "Basic token123"},
			{"only bearer", "Bearer"},
			{"multiple spaces", "Bearer  token  extra"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				w := httptest.NewRecorder()
				router := srv.router()

				req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
				req.Header.Set("Authorization", tt.header)
				req = addHostContext(req)
				router.ServeHTTP(w, req)

				require.Equal(t, http.StatusUnauthorized, w.Code)
			})
		}
	})

	t.Run("rejects malformed token", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer not.a.valid.jwt.token")
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		require.Contains(t, resp["msg"], "invalid or expired token")
	})

	t.Run("rejects expired access token", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		expiredTime := now.Add(-1 * time.Hour)

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"sub":   "did:plc:authmw3",
			"iat":   expiredTime.UTC().Unix(),
			"exp":   expiredTime.UTC().Unix(),
			"jti":   "test-jti-expired",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+accessString)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects access token for refresh endpoint", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor("did:plc:authmw4", "authmw4@example.com", "authmw4.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		require.Contains(t, resp["msg"], "invalid or expired token")
	})

	t.Run("rejects refresh token for access endpoint", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor("did:plc:authmw5", "authmw5@example.com", "authmw5.dev.atlaspds.net")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.RefreshToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects token with wrong audience", func(t *testing.T) {
		t.Parallel()

		now := time.Now()

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   "did:plc:wrong-service",
			"sub":   "did:plc:authmw6",
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-wrongaud",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+accessString)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects token signed with wrong key", func(t *testing.T) {
		t.Parallel()

		wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		now := time.Now()

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"sub":   "did:plc:authmw7",
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-wrongkey",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(wrongKey)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+accessString)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("rejects token for non-existent actor", func(t *testing.T) {
		t.Parallel()

		now := time.Now()

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"sub":   "did:plc:nonexistent",
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-noactor",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.getSession", nil)
		req.Header.Set("Authorization", "Bearer "+accessString)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)

		var resp map[string]any
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		require.Contains(t, resp["msg"], "actor not found")
	})

	t.Run("rejects refresh token not in actor's token list", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor("did:plc:authmw8", "authmw8@example.com", "authmw8.dev.atlaspds.net")

		// remove the refresh token from the actor's list
		actor.RefreshTokens = []*types.RefreshToken{}
		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil)
		req.Header.Set("Authorization", "Bearer "+session.RefreshToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)

		var resp map[string]any
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		require.Contains(t, resp["msg"], "refresh token not found")
	})

	t.Run("rejects refresh token expired in database but valid JWT", func(t *testing.T) {
		t.Parallel()

		pwHash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
		require.NoError(t, err)

		now := time.Now()
		expiredTime := now.Add(-1 * time.Hour)

		// create a JWT that is still valid, but expired in the database
		refreshClaims := jwt.MapClaims{
			"scope": "com.atproto.refresh",
			"aud":   srv.hosts[testPDSHost].serviceDID,
			"sub":   "did:plc:authmw9",
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(refreshTokenTTL).UTC().Unix(), // JWT not expired
			"jti":   "test-jti-db-expired",
		}

		refreshToken := jwt.NewWithClaims(jwt.SigningMethodES256, refreshClaims)
		refreshString, err := refreshToken.SignedString(signingKey)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            "did:plc:authmw9",
			Email:          "authmw9@example.com",
			Handle:         "authmw9.dev.atlaspds.net",
			PdsHost:        testPDSHost,
			PasswordHash:   pwHash,
			EmailConfirmed: true,
			CreatedAt:      timestamppb.Now(),
			Active:         true,
			SigningKey:     []byte("signing_key"),
			RotationKeys:   [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{
				{
					Token:     refreshString,
					CreatedAt: timestamppb.New(now),
					ExpiresAt: timestamppb.New(expiredTime), // expired in database
				},
			},
		}

		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.refreshSession", nil)
		req.Header.Set("Authorization", "Bearer "+refreshString)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)

		var resp map[string]any
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		require.Contains(t, resp["msg"], "refresh token expired")
	})

	t.Run("sets actor and token in context", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor("did:plc:authmw10", "authmw10@example.com", "authmw10.dev.atlaspds.net")

		// create a handler that checks the context
		var capturedActor *types.Actor
		var capturedToken string

		testHandler := srv.authMiddleware(func(w http.ResponseWriter, r *http.Request) {
			capturedActor = actorFromContext(r.Context())
			capturedToken = tokenFromContext(r.Context())
			w.WriteHeader(http.StatusOK)
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)

		testHandler(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.NotNil(t, capturedActor)
		require.Equal(t, actor.Did, capturedActor.Did)
		require.Equal(t, actor.Handle, capturedActor.Handle)
		require.Equal(t, session.AccessToken, capturedToken)
	})
}
