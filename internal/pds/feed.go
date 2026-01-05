package pds

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/pds/db"
	"github.com/jcalabro/atlas/internal/types"
)

const getFeedSkeletonLxm = "app.bsky.feed.getFeedSkeleton"

// handleGetFeed proxies feed requests to the appview.
// If the user is authenticated, a service auth token is created for the feed generator.
func (s *server) handleGetFeed(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	feed := r.URL.Query().Get("feed")
	if feed == "" {
		s.badRequest(w, fmt.Errorf("feed parameter is required"))
		return
	}

	// validate and parse the feed URI
	feedURI, err := at.ParseURI(feed)
	if err != nil {
		s.badRequest(w, fmt.Errorf("invalid feed URI: %w", err))
		return
	}

	if s.appviewProxy == nil {
		s.notFound(w, fmt.Errorf("no appview configured for feed requests"))
		return
	}

	// fetch the feed generator record to get the feed generator DID
	feedGeneratorDID, err := s.appviewProxy.getFeedGenerator(ctx, feedURI.Repo, feedURI.Collection, feedURI.Rkey)
	if err != nil {
		s.log.Error("failed to fetch feed generator", "err", err, "feed", feed)
		s.notFound(w, fmt.Errorf("feed not found"))
		return
	}

	// check if user is authenticated (optional auth)
	actor := s.tryGetAuthenticatedActor(r)

	var serviceAuthToken string
	if actor != nil {
		// create service auth token for the feed generator
		token, err := createServiceAuthToken(actor, feedGeneratorDID, getFeedSkeletonLxm)
		if err != nil {
			s.log.Error("failed to create service auth token", "err", err, "did", actor.Did)
			s.internalErr(w, fmt.Errorf("authentication error"))
			return
		}
		serviceAuthToken = token
	}

	// proxy the request with the optional service auth token
	if err := s.appviewProxy.proxyWithAuth(w, r, serviceAuthToken); err != nil {
		s.log.Error("failed to proxy getFeed request", "err", err, "feed", feed)
		s.internalErr(w, fmt.Errorf("proxy error"))
	}
}

// tryGetAuthenticatedActor attempts to authenticate the user from the Authorization header.
// Returns nil if no valid authentication is present (this is not an error for optional auth).
func (s *server) tryGetAuthenticatedActor(r *http.Request) *types.Actor {
	ctx := r.Context()

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil
	}

	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		return nil
	}

	tokenString := parts[1]

	claims, err := s.verifyAccessToken(ctx, tokenString)
	if err != nil {
		s.log.Debug("failed to verify access token for optional auth", "err", err)
		return nil
	}

	actor, err := s.db.GetActorByDID(ctx, claims.DID)
	if errors.Is(err, db.ErrNotFound) {
		return nil
	}
	if err != nil {
		s.log.Error("failed to get actor by DID for optional auth", "did", claims.DID, "err", err)
		return nil
	}

	// verify the actor belongs to the requested PDS host
	host := hostFromContext(ctx)
	if host != nil && actor.PdsHost != host.hostname {
		return nil
	}

	return actor
}
