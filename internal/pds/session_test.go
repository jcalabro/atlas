package pds

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCreateSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.signingKey = signingKey
	srv.serviceDID = "did:plc:test-service-12345"

	t.Run("creates valid access and refresh tokens", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)
		require.NotNil(t, session)

		assert.NotEmpty(t, session.AccessToken)
		assert.NotEmpty(t, session.RefreshToken)

		accessToken, err := jwt.Parse(session.AccessToken, func(token *jwt.Token) (any, error) {
			return &signingKey.PublicKey, nil
		})
		require.NoError(t, err)
		require.True(t, accessToken.Valid)

		accessClaims, ok := accessToken.Claims.(jwt.MapClaims)
		require.True(t, ok)
		assert.Equal(t, "com.atproto.access", accessClaims["scope"])
		assert.Equal(t, "did:plc:test-service-12345", accessClaims["aud"])
		assert.Equal(t, "did:plc:testuser123", accessClaims["sub"])
		assert.NotEmpty(t, accessClaims["jti"])

		refreshToken, err := jwt.Parse(session.RefreshToken, func(token *jwt.Token) (any, error) {
			return &signingKey.PublicKey, nil
		})
		require.NoError(t, err)
		require.True(t, refreshToken.Valid)

		refreshClaims, ok := refreshToken.Claims.(jwt.MapClaims)
		require.True(t, ok)
		assert.Equal(t, "com.atproto.refresh", refreshClaims["scope"])
		assert.Equal(t, "did:plc:test-service-12345", refreshClaims["aud"])
		assert.Equal(t, "did:plc:testuser123", refreshClaims["sub"])
		assert.NotEmpty(t, refreshClaims["jti"])

		assert.Equal(t, accessClaims["jti"], refreshClaims["jti"])
	})

	t.Run("saves refresh token to actor", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser456",
			Email:         "test2@example.com",
			Handle:        "test2.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		retrievedActor, err := srv.db.GetActorByEmail(ctx, actor.Email)
		require.NoError(t, err)
		require.NotNil(t, retrievedActor)

		require.Len(t, retrievedActor.RefreshTokens, 1)
		assert.Equal(t, session.RefreshToken, retrievedActor.RefreshTokens[0].Token)
		assert.NotNil(t, retrievedActor.RefreshTokens[0].CreatedAt)
		assert.NotNil(t, retrievedActor.RefreshTokens[0].ExpiresAt)

		expiresAt := retrievedActor.RefreshTokens[0].ExpiresAt.AsTime()
		expectedExpiry := time.Now().Add(refreshTokenTTL)
		assert.WithinDuration(t, expectedExpiry, expiresAt, 5*time.Second)
	})

	t.Run("supports multiple refresh tokens per actor", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser789",
			Email:         "test3@example.com",
			Handle:        "test3.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

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

		assert.Equal(t, session1.RefreshToken, retrievedActor.RefreshTokens[0].Token)
		assert.Equal(t, session2.RefreshToken, retrievedActor.RefreshTokens[1].Token)
		assert.NotEqual(t, session1.RefreshToken, session2.RefreshToken)
		assert.NotEqual(t, session1.AccessToken, session2.AccessToken)
	})

	t.Run("access token expires in 3 hours", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser101112",
			Email:         "test4@example.com",
			Handle:        "test4.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

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
		assert.WithinDuration(t, expectedExpiry, expTime, 5*time.Second)
	})

	t.Run("refresh token expires in 7 days", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:testuser131415",
			Email:         "test5@example.com",
			Handle:        "test5.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

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
		assert.WithinDuration(t, expectedExpiry, expTime, 5*time.Second)
	})
}

func TestVerifyAccessToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.signingKey = signingKey
	srv.serviceDID = "did:plc:test-service-12345"

	t.Run("verifies valid access token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		claims, err := srv.verifyAccessToken(ctx, session.AccessToken)
		require.NoError(t, err)
		require.NotNil(t, claims)

		assert.Equal(t, "did:plc:testuser123", claims.DID)
		assert.Equal(t, "com.atproto.access", claims.Scope)
		assert.NotEmpty(t, claims.JTI)
	})

	t.Run("rejects refresh token when expecting access token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser456",
			Email:         "test2@example.com",
			Handle:        "test2.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(ctx, session.RefreshToken)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid scope")
	})

	t.Run("rejects expired access token", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		expiredTime := now.Add(-1 * time.Hour)

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.serviceDID,
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
		assert.Contains(t, err.Error(), "token is expired")
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
		assert.Contains(t, err.Error(), "invalid audience")
	})

	t.Run("rejects token signed with wrong key", func(t *testing.T) {
		t.Parallel()

		wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		now := time.Now()

		accessClaims := jwt.MapClaims{
			"scope": "com.atproto.access",
			"aud":   srv.serviceDID,
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
			"aud":   srv.serviceDID,
			"iat":   now.UTC().Unix(),
			"exp":   now.Add(accessTokenTTL).UTC().Unix(),
			"jti":   "test-jti-abc",
		}

		accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
		accessString, err := accessToken.SignedString(signingKey)
		require.NoError(t, err)

		_, err = srv.verifyAccessToken(ctx, accessString)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing or invalid sub claim")
	})
}

func TestVerifyRefreshToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.signingKey = signingKey
	srv.serviceDID = "did:plc:test-service-12345"

	t.Run("verifies valid refresh token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser123",
			Email:         "test@example.com",
			Handle:        "test.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		claims, err := srv.verifyRefreshToken(ctx, session.RefreshToken)
		require.NoError(t, err)
		require.NotNil(t, claims)

		assert.Equal(t, "did:plc:testuser123", claims.DID)
		assert.Equal(t, "com.atproto.refresh", claims.Scope)
		assert.NotEmpty(t, claims.JTI)
	})

	t.Run("rejects access token when expecting refresh token", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser456",
			Email:         "test2@example.com",
			Handle:        "test2.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		_, err = srv.verifyRefreshToken(ctx, session.AccessToken)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid scope")
	})

	t.Run("rejects expired refresh token", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		expiredTime := now.Add(-8 * 24 * time.Hour)

		refreshClaims := jwt.MapClaims{
			"scope": "com.atproto.refresh",
			"aud":   srv.serviceDID,
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
		assert.Contains(t, err.Error(), "token is expired")
	})

	t.Run("access and refresh tokens have matching JTI", func(t *testing.T) {
		t.Parallel()

		actor := &types.Actor{
			Did:           "did:plc:testuser101112",
			Email:         "test3@example.com",
			Handle:        "test3.bsky.social",
			CreatedAt:     timestamppb.Now(),
			Active:        true,
			RefreshTokens: []*types.RefreshToken{},
		}

		if err := srv.db.SaveActor(ctx, actor); err != nil {
			t.Fatalf("failed to save actor: %v", err)
		}

		session, err := srv.createSession(ctx, actor)
		require.NoError(t, err)

		accessClaims, err := srv.verifyAccessToken(ctx, session.AccessToken)
		require.NoError(t, err)

		refreshClaims, err := srv.verifyRefreshToken(ctx, session.RefreshToken)
		require.NoError(t, err)

		assert.Equal(t, accessClaims.JTI, refreshClaims.JTI)
		assert.Equal(t, accessClaims.DID, refreshClaims.DID)
	})
}
