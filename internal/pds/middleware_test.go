package pds

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestObservabilityMiddleware(t *testing.T) {
	t.Parallel()

	s := &server{
		log:    slog.Default(),
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	handler := s.observabilityMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("test response"))
		require.NoError(t, err)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("User-Agent", "test-agent")
	rw := httptest.NewRecorder()

	handler.ServeHTTP(rw, req)

	require.Equal(t, http.StatusOK, rw.Code)
	require.Equal(t, "test response", rw.Body.String())
}

func TestObservabilityMiddlewareErrorStatus(t *testing.T) {
	t.Parallel()

	s := &server{
		log:    slog.Default(),
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	handler := s.observabilityMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("error"))
		require.NoError(t, err)
	}))

	req := httptest.NewRequest(http.MethodPost, "/error", nil)
	rw := httptest.NewRecorder()

	handler.ServeHTTP(rw, req)

	require.Equal(t, http.StatusInternalServerError, rw.Code)
	require.Equal(t, "error", rw.Body.String())
}

func TestSpanFromContext(t *testing.T) {
	t.Parallel()

	s := &server{
		log:    slog.Default(),
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	handler := s.observabilityMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := spanFromContext(r.Context())
		require.NotNil(t, span, "expected span from context")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rw := httptest.NewRecorder()

	handler.ServeHTTP(rw, req)
}
