package pds

import (
	"fmt"
	"net/http"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/jcalabro/atlas/internal/util"
)

func (s *server) handleListRepos(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	host := hostFromContext(ctx)

	cursor := r.URL.Query().Get("cursor")
	if cursor != "" {
		if _, err := syntax.ParseDID(cursor); err != nil {
			s.badRequest(w, fmt.Errorf("invalid cursor (must be a did)"))
			return
		}
	}

	limit, err := parseIntParam(r, "limit", 500)
	if err != nil || limit < 0 {
		s.badRequest(w, fmt.Errorf("invalid limit"))
		return
	}
	if limit > 500 {
		limit = 500 // set the max scan size
	}

	actors, next, err := s.db.ListActors(ctx, host.hostname, cursor, limit)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to list repos: %w", err))
		return
	}

	repos := make([]*atproto.SyncListRepos_Repo, len(actors))
	for ndx, actor := range actors {
		repos[ndx] = &atproto.SyncListRepos_Repo{
			Active: util.Ptr(actor.Active),
			Did:    actor.Did,
			Head:   actor.Head,
			Rev:    actor.Rev,
		}
	}

	s.jsonOK(w, atproto.SyncListRepos_Output{
		Cursor: nextCursorOrNil(next),
		Repos:  repos,
	})
}
