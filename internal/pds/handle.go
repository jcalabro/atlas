package pds

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"go.opentelemetry.io/otel/attribute"
)

func (s *server) handleResolveHandle(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "handleResolveHandle")
	defer span.End()

	raw := r.URL.Query().Get("handle")
	span.SetAttributes(attribute.String("handle", raw))

	if raw == "" {
		s.badRequest(w, fmt.Errorf("handle is required"))
		return
	}

	handle, err := syntax.ParseHandle(raw)
	if err != nil {
		s.badRequest(w, fmt.Errorf("invalid handle: %w", err))
		return
	}

	ident, err := s.directory.LookupHandle(ctx, handle)
	if errors.Is(err, identity.ErrHandleNotFound) {
		s.notFound(w, fmt.Errorf("handle %q not found", raw))
		return
	}
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to resolve handle to did: %w", err))
		return
	}

	type response struct {
		DID string `json:"did"`
	}

	s.jsonOK(w, &response{DID: ident.DID.String()})
}
