package pds

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (s *server) handleGetPreferences(w http.ResponseWriter, r *http.Request) {
	actor := actorFromContext(r.Context())

	var prefs map[string]any
	if err := json.Unmarshal(actor.Preferences, &prefs); err != nil || prefs["preferences"] == nil {
		prefs = map[string]any{
			"preferences": []any{},
		}
	}

	s.jsonOK(w, prefs)
}

func (s *server) handlePutPreferences(w http.ResponseWriter, r *http.Request) {
	actor := actorFromContext(r.Context())

	var prefs map[string]any
	if err := json.NewDecoder(r.Body).Decode(&prefs); err != nil {
		s.badRequest(w, fmt.Errorf("invalid preferences json: %w", err))
		return
	}

	b, err := json.Marshal(prefs)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to marshal preferences: %w", err))
		return
	}

	actor.Preferences = b
	if err := s.db.SaveActor(r.Context(), actor); err != nil {
		s.internalErr(w, fmt.Errorf("failed to save preferences: %w", err))
		return
	}

	w.WriteHeader(http.StatusOK)
}
