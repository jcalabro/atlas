package pds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jcalabro/atlas/internal/pds/db"
	"github.com/jcalabro/atlas/internal/pds/metrics"
	"github.com/jcalabro/atlas/internal/types"
	"golang.org/x/crypto/bcrypt"
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

func (s *server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	host := hostFromContext(ctx)

	// metric status: empty means don't record (validation errors), otherwise records on return
	metricStatus := ""
	defer func() {
		if metricStatus != "" {
			metrics.AuthAttempts.WithLabelValues("login", metricStatus).Inc()
		}
	}()

	var in atproto.ServerCreateSession_Input
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		s.badRequest(w, fmt.Errorf("invalid request body: %w", err))
		return
	}

	identifier := strings.ToLower(in.Identifier)
	if in.Identifier == "" {
		s.badRequest(w, fmt.Errorf("identifier is required"))
		return
	}
	if in.Password == "" {
		s.badRequest(w, fmt.Errorf("password is required"))
		return
	}

	// past validation - start recording metrics (default to failure for auth)
	metricStatus = "failure"

	var (
		actor *types.Actor
		err   error
	)

	if strings.HasPrefix(identifier, "did:") {
		// try parsing as DID first
		if _, parseErr := syntax.ParseDID(identifier); parseErr == nil {
			actor, err = s.db.GetActorByDID(ctx, identifier)
		}
	} else {
		// try parsing as handle (handles are globally unique)
		if handle, parseErr := syntax.ParseHandle(identifier); parseErr == nil {
			actor, err = s.db.GetActorByHandle(ctx, handle.String())
		} else {
			// fall back to email (per-PDS unique)
			actor, err = s.db.GetActorByEmail(ctx, host.hostname, identifier)
		}
	}
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		metricStatus = "error"
		s.internalErr(w, fmt.Errorf("failed to lookup account: %w", err))
		return
	}

	if actor == nil || errors.Is(err, db.ErrNotFound) {
		s.badRequest(w, fmt.Errorf("invalid account identifier or password"))
		return
	}

	// verify the actor belongs to this PDS host
	if actor.PdsHost != host.hostname {
		s.badRequest(w, fmt.Errorf("invalid account identifier or password"))
		return
	}

	if err := bcrypt.CompareHashAndPassword(actor.PasswordHash, []byte(in.Password)); err != nil {
		s.badRequest(w, fmt.Errorf("invalid identifier or password"))
		return
	}

	session, err := s.createSession(r.Context(), actor)
	if err != nil {
		metricStatus = "error"
		s.log.Error("failed to create session", "did", actor.Did, "err", err)
		s.internalErr(w, fmt.Errorf("failed to create session"))
		return
	}

	metricStatus = "success"

	var status *string
	if !actor.Active {
		deactivated := "deactivated"
		status = &deactivated
	}

	resp := &atproto.ServerCreateSession_Output{
		AccessJwt:       session.AccessToken,
		RefreshJwt:      session.RefreshToken,
		Handle:          actor.Handle,
		Did:             actor.Did,
		Email:           &actor.Email,
		EmailConfirmed:  &actor.EmailConfirmed,
		EmailAuthFactor: new(bool), // not implemented
		Active:          &actor.Active,
		Status:          status,
	}

	s.jsonOK(w, resp)
}

func (s *server) createSession(ctx context.Context, actor *types.Actor) (*Session, error) {
	ctx, span := s.tracer.Start(ctx, "createSession")
	defer span.End()

	host := hostFromContext(ctx)
	if host == nil {
		return nil, fmt.Errorf("host config not found in context")
	}

	now := time.Now()
	accexp := now.Add(accessTokenTTL)
	refexp := now.Add(refreshTokenTTL)
	jti := uuid.NewString()

	accessClaims := jwt.MapClaims{
		"scope": "com.atproto.access",
		"aud":   host.serviceDID,
		"sub":   actor.Did,
		"iat":   now.UTC().Unix(),
		"exp":   accexp.UTC().Unix(),
		"jti":   jti,
	}

	refreshClaims := jwt.MapClaims{
		"scope": "com.atproto.refresh",
		"aud":   host.serviceDID,
		"sub":   actor.Did,
		"iat":   now.UTC().Unix(),
		"exp":   refexp.UTC().Unix(),
		"jti":   jti,
	}

	accessToken := jwt.NewWithClaims(jwt.SigningMethodES256, accessClaims)
	accessString, err := accessToken.SignedString(host.signingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign access token: %w", err)
	}

	refreshToken := jwt.NewWithClaims(jwt.SigningMethodES256, refreshClaims)
	refreshString, err := refreshToken.SignedString(host.signingKey)
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
	ctx, span := s.tracer.Start(ctx, "verifyToken")
	defer span.End()

	host := hostFromContext(ctx)
	if host == nil {
		return nil, fmt.Errorf("host config not found in context")
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodECDSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return &host.signingKey.PublicKey, nil
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
	if aud != host.serviceDID {
		return nil, fmt.Errorf("invalid audience: expected %s, got %s", host.serviceDID, aud)
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

func (s *server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	actor := actorFromContext(r.Context())
	if actor == nil {
		s.internalErr(w, fmt.Errorf("actor not found in context"))
		return
	}

	var status *string
	if !actor.Active {
		deactivated := "deactivated"
		status = &deactivated
	}

	resp := &atproto.ServerGetSession_Output{
		Handle:          actor.Handle,
		Did:             actor.Did,
		Email:           &actor.Email,
		EmailConfirmed:  &actor.EmailConfirmed,
		EmailAuthFactor: new(bool), // not implemented
		Active:          &actor.Active,
		Status:          status,
	}

	s.jsonOK(w, resp)
}

func (s *server) handleRefreshSession(w http.ResponseWriter, r *http.Request) {
	// default to error - already past auth middleware so all paths should record
	metricStatus := "error"
	defer func() {
		metrics.AuthAttempts.WithLabelValues("refresh", metricStatus).Inc()
	}()

	actor := actorFromContext(r.Context())
	if actor == nil {
		s.internalErr(w, fmt.Errorf("actor not found in context"))
		return
	}

	refreshToken := tokenFromContext(r.Context())
	if refreshToken == "" {
		s.internalErr(w, fmt.Errorf("refresh token not found in context"))
		return
	}

	// remove the old refresh token from the actor's list
	newRefreshTokens := make([]*types.RefreshToken, 0, len(actor.RefreshTokens))
	for _, rt := range actor.RefreshTokens {
		if rt.Token != refreshToken {
			newRefreshTokens = append(newRefreshTokens, rt)
		}
	}
	actor.RefreshTokens = newRefreshTokens

	// create a new session
	session, err := s.createSession(r.Context(), actor)
	if err != nil {
		s.log.Error("failed to create new session for refresh", "did", actor.Did, "error", err)
		s.internalErr(w, fmt.Errorf("failed to create session"))
		return
	}

	metricStatus = "success"

	var status *string
	if !actor.Active {
		deactivated := "deactivated"
		status = &deactivated
	}

	resp := &atproto.ServerRefreshSession_Output{
		AccessJwt:  session.AccessToken,
		RefreshJwt: session.RefreshToken,
		Handle:     actor.Handle,
		Did:        actor.Did,
		Active:     &actor.Active,
		Status:     status,
	}

	s.jsonOK(w, resp)
}

func (s *server) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	actor := actorFromContext(ctx)
	if actor == nil {
		s.internalErr(w, fmt.Errorf("actor not found in context"))
		return
	}

	accessToken := tokenFromContext(ctx)
	if accessToken == "" {
		s.internalErr(w, fmt.Errorf("access token not found in context"))
		return
	}

	// verify and extract the refresh token JTI from the access token
	claims, err := s.verifyAccessToken(ctx, accessToken)
	if err != nil {
		s.log.Error("failed to verify access token", "error", err)
		s.internalErr(w, fmt.Errorf("failed to verify token"))
		return
	}

	// remove the refresh token that matches this JTI
	newRefreshTokens := make([]*types.RefreshToken, 0, len(actor.RefreshTokens))
	for _, rt := range actor.RefreshTokens {
		rtClaims, err := s.verifyRefreshToken(ctx, rt.Token)
		if err != nil {
			continue // skip invalid tokens
		}

		if rtClaims.JTI != claims.JTI {
			// keep tokens that don't match the JTI
			newRefreshTokens = append(newRefreshTokens, rt)
		}
	}
	actor.RefreshTokens = newRefreshTokens

	// save the updated actor
	if err := s.db.SaveActor(ctx, actor); err != nil {
		s.log.Error("failed to save actor after deleting session", "did", actor.Did, "error", err)
		s.internalErr(w, fmt.Errorf("failed to delete session"))
		return
	}
}
