package pds

import (
	"net/http"

	"github.com/jcalabro/atlas/internal/env"
)

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	type response struct {
		Version string `json:"version"`
	}

	s.writeJSON(w, response{Version: env.Version})
}
