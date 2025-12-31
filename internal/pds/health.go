package pds

import (
	"net/http"

	"github.com/jcalabro/atlas/internal/env"
	"go.opentelemetry.io/otel/trace"
)

func handlePing(s *server, span trace.Span, w http.ResponseWriter, r *http.Request) {
	s.plaintextOK(w, "OK")
}

func handleHealth(s *server, span trace.Span, w http.ResponseWriter, r *http.Request) {
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
