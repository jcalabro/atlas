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
	srv.cfg.signingKey = signingKey
	srv.cfg.serviceDID = "did:plc:test-service-12345"

	t.Run("creates valid access and refresh tokens", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.dev.atlaspds.net",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
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
			Email:         "test2@example.com",
			Handle:        "test2.dev.atlaspds.net",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		retrievedActor, err := srv.db.GetActorByEmail(ctx, actor.Email)
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
			Email:         "test3@example.com",
			Handle:        "test3.dev.atlaspds.net",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session1, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		retrievedActor, err := srv.db.GetActorByEmail(ctx, actor.Email)
		require.NoError(t, err)
		require.Len(t, retrievedActor.RefreshTokens, 1)

		session2, err := srv.createSession(ctx, retrievedActor)
		require.NoError(t, err)

		retrievedActor, err = srv.db.GetActorByEmail(ctx, actor.Email)
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
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
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
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
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
	srv.cfg.signingKey = signingKey
	srv.cfg.serviceDID = "did:plc:test-service-12345"

	t.Run("verifies valid access token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.dev.atlaspds.net",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		claims, err := srv.verifyAccessToken(ctx, session.AccessToken)
		require.NoError(t, err)
		require.NotNil(t, claims)

		require.Equal(t, "did:plc:testuser123", claims.DID)
		require.Equal(t, "com.atproto.access", claims.Scope)
		require.NotEmpty(t, claims.JTI)
	})

	t.Run("rejects refresh token when expecting access token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser456",
			Email:         "test2@example.com",
			Handle:        "test2.dev.atlaspds.net",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(ctx, session.RefreshToken)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid scope")
	})

	t.Run("rejects expired access token", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		expiredTime := now.Add(-1 * time.Hour)

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.cfg.serviceDID,
			"sub":   "did:plc:testuser789",
			"iat":   expiredTime.UTC().Unix(),
			"exp":   expiredTime.UTC().Unix(),
			"jti":   "test-jti-123",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(ctx, accessString)
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

		_, err = srv.verifyAccessToken(ctx, accessString)
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
			"aud":   srv.cfg.serviceDID,
			"sub":   "did:plc:testuser131415",
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-789",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(wrongKey)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(ctx, accessString)
		require.Error(t, err)
	})

	t.Run("rejects malformed token", func(t *testing.T) {
		t.Parallel()

		_, err := srv.verifyAccessToken(ctx, "not.a.valid.jwt")
		require.Error(t, err)
	})

	t.Run("rejects token with missing claims", func(t *testing.T) {
		t.Parallel()

		now := time.Now()

		// Missing sub claim
		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.cfg.serviceDID,
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-abc",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(ctx, accessString)
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
	srv.cfg.signingKey = signingKey
	srv.cfg.serviceDID = "did:plc:test-service-12345"

	t.Run("verifies valid refresh token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.dev.atlaspds.net",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		claims, err := srv.verifyRefreshToken(ctx, session.RefreshToken)
		require.NoError(t, err)
		require.NotNil(t, claims)

		require.Equal(t, "did:plc:testuser123", claims.DID)
		require.Equal(t, "com.atproto.refresh", claims.Scope)
		require.NotEmpty(t, claims.JTI)
	})

	t.Run("rejects access token when expecting refresh token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser456",
			Email:         "test2@example.com",
			Handle:        "test2.dev.atlaspds.net",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		_, err = srv.verifyRefreshToken(ctx, session.AccessToken)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid scope")
	})

	t.Run("rejects expired refresh token", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		expiredTime := now.Add(-8 * 24 * time.Hour)

		refreshClaims := jwt.MapClaims{
			"scope": "com.atproto.refresh",
			"aud":   srv.cfg.serviceDID,
			"sub":   "did:plc:testuser789",
			"iat":   expiredTime.UTC().Unix(),
			"exp":   expiredTime.UTC().Unix(),
			"jti":   "test-jti-123",
		}

		refreshToken := jwt.NewWithClaims(jwt.SigningMethodES256, refreshClaims)
		refreshString, err := refreshToken.SignedString(signingKey)
		require.NoError(t, err)

		_, err = srv.verifyRefreshToken(ctx, refreshString)
		require.Error(t, err)
		require.Contains(t, err.Error(), "token is expired")
	})

	t.Run("access and refresh tokens have matching JTI", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser101112",
			Email:         "test3@example.com",
			Handle:        "test3.dev.atlaspds.net",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			PasswordHash:  []byte("password_hash"),
			SigningKey:    []byte("signing_key"),
			RotationKeys:  [][]byte{[]byte("rotation_key")},
			RefreshTokens: []*types.RefreshToken{},
		}

		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		accessClaims, err := srv.verifyAccessToken(ctx, session.AccessToken)
		require.NoError(t, err)

		refreshClaims, err := srv.verifyRefreshToken(ctx, session.RefreshToken)
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
	srv.cfg.signingKey = signingKey
	srv.cfg.serviceDID = "did:plc:test-service-12345"

	setupTestActor := func(did, email, handle, password string) *types.Actor {
		pwHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		require.NoError(t, err)

		actor := &types.Actor{
			Did:            did,
			Email:          email,
			Handle:         handle,
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

	t.Run("creates session with DID identifier", func(t *testing.T) {
		t.Parallel()

		actor := setupTestActor("did:plc:testuser1", "test1@example.com", "test1.dev.atlaspds.net", "password123")

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"did:plc:testuser1","password":"password123"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
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
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("rejects non-existent user", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"nonexistent@example.com","password":"password"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("rejects missing identifier", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"password":"password"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("rejects missing password", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		reqBody := `{"identifier":"test@example.com"}`
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createSession", strings.NewReader(reqBody))
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
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		require.Equal(t, false, resp["active"])
		require.Equal(t, "deactivated", resp["status"])
	})
}
