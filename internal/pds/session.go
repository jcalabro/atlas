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
	_, span := s.tracer.Start(ctx, "pds/createSession")
	defer span.End()

	now := time.Now()
	accexp := now.Add(accessTokenTTL)
	refexp := now.Add(refreshTokenTTL)
	jti := uuid.NewString()

	accessClaims := jwt.MapClaims{
		"scope": "com.atproto.access",
		"aud":   s.serviceDID,
		"sub":   actor.Did,
		"iat":   now.UTC().Unix(),
		"exp":   accexp.UTC().Unix(),
		"jti":   jti,
	}

	refreshClaims := jwt.MapClaims{
		"scope": "com.atproto.refresh",
		"aud":   s.serviceDID,
		"sub":   actor.Did,
		"iat":   now.UTC().Unix(),
		"exp":   refexp.UTC().Unix(),
		"jti":   jti,
	}

	accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
	accessString, err := accessToken.SignedString(s.signingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign access token: %w", err)
	}

	refreshToken := jwt.NewWithClaims(jwt.SigningMethodES256, refreshClaims)
	refreshString, err := refreshToken.SignedString(s.signingKey)
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
