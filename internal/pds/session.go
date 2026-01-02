package pds

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jcalabro/atlas/internal/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	accessTokenTTL  = 3 * time.Hour
	refreshTokenTTL = 7 * 24 * time.Hour
)

type Session struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
}

func (s *server) createSession(ctx context.Context, actor *types.Actor) (*Session, error) {
	_, span := s.tracer.Start(ctx, "createSession")
	defer span.End()

	now := time.Now()
	accexp := now.Add(accessTokenTTL)
	refexp := now.Add(refreshTokenTTL)
	jti := uuid.NewString()

	accessClaims := jwt.MapClaims{
		"scope": "com.atproto.access",
		"aud":   s.cfg.serviceDID,
		"sub":   actor.Did,
		"iat":   now.UTC().Unix(),
		"exp":   accexp.UTC().Unix(),
		"jti":   jti,
	}

	refreshClaims := jwt.MapClaims{
		"scope": "com.atproto.refresh",
		"aud":   s.cfg.serviceDID,
		"sub":   actor.Did,
		"iat":   now.UTC().Unix(),
		"exp":   refexp.UTC().Unix(),
		"jti":   jti,
	}

	accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
	accessString, err := accessToken.SignedString(s.cfg.signingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign access token: %w", err)
	}

	refreshToken := jwt.NewWithClaims(jwt.SigningMethodES256, refreshClaims)
	refreshString, err := refreshToken.SignedString(s.cfg.signingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign refresh token: %w", err)
	}

	actor.RefreshTokens = append(actor.RefreshTokens, &types.RefreshToken{
		Token:     refreshString,
		CreatedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(refexp),
	})

	if err := s.db.SaveActor(ctx, actor); err != nil {
		return nil, fmt.Errorf("failed to save actor with refresh token: %w", err)
	}

	return &Session{
		AccessToken:  accessString,
		RefreshToken: refreshString,
	}, nil
}

// VerifiedClaims contains the verified claims from a token
type VerifiedClaims struct {
	DID   string
	JTI   string
	Scope string
}

func (s *server) verifyAccessToken(ctx context.Context, tokenString string) (*VerifiedClaims, error) {
	return s.verifyToken(ctx, tokenString, "com.atproto.access")
}

func (s *server) verifyRefreshToken(ctx context.Context, tokenString string) (*VerifiedClaims, error) {
	return s.verifyToken(ctx, tokenString, "com.atproto.refresh")
}

func (s *server) verifyToken(ctx context.Context, tokenString string, expectedScope string) (*VerifiedClaims, error) {
	_, span := s.tracer.Start(ctx, "verifyToken")
	defer span.End()

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodECDSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return &s.cfg.signingKey.PublicKey, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("token is invalid")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("failed to parse claims")
	}

	scope, ok := claims["scope"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid scope claim")
	}
	if scope != expectedScope {
		return nil, fmt.Errorf("invalid scope: expected %s, got %s", expectedScope, scope)
	}

	aud, ok := claims["aud"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid aud claim")
	}
	if aud != s.cfg.serviceDID {
		return nil, fmt.Errorf("invalid audience: expected %s, got %s", s.cfg.serviceDID, aud)
	}

	sub, ok := claims["sub"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid sub claim")
	}

	jti, ok := claims["jti"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid jti claim")
	}

	return &VerifiedClaims{
		DID:   sub,
		JTI:   jti,
		Scope: scope,
	}, nil
}
