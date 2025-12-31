package pds

import (
	"net/http"

	"github.com/jcalabro/atlas/internal/env"
)

func (s *server) handlePing(w http.ResponseWriter, r *http.Request) {
	s.plaintextOK(w, "OK")
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	type response struct {
		Version string `json:"version"`
	}

	status := http.StatusOK
	if err := s.db.Ping(r.Context()); err != nil {
		s.log.Error("failed to ping foundation", "err", err)
		status = http.StatusInternalServerError
	}

	s.jsonWithCode(w, status, &response{Version: env.Version})
}
