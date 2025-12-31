package pds

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/trace/noop"
)

func TestObservabilityMiddleware(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	s := &server{
		log:    logger,
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	handler := s.observabilityMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("test response"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("User-Agent", "test-agent")
	rw := httptest.NewRecorder()

	handler.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rw.Code)
	}

	logs := logBuf.String()
	if logs == "" {
		t.Error("expected debug logs, got none")
	}

	if !contains(logs, "incoming request") {
		t.Error("expected 'incoming request' log")
	}

	if !contains(logs, "request completed") {
		t.Error("expected 'request completed' log")
	}

	if !contains(logs, "GET") {
		t.Error("expected method in logs")
	}

	if !contains(logs, "/test") {
		t.Error("expected path in logs")
	}
}

func TestObservabilityMiddlewareErrorStatus(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	s := &server{
		log:    logger,
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	handler := s.observabilityMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("error"))
	}))

	req := httptest.NewRequest(http.MethodPost, "/error", nil)
	rw := httptest.NewRecorder()

	handler.ServeHTTP(rw, req)

	if rw.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, rw.Code)
	}

	logs := logBuf.String()
	if !contains(logs, "500") {
		t.Error("expected status code in logs")
	}
}

func TestSpanFromContext(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	s := &server{
		log:    logger,
		tracer: noop.NewTracerProvider().Tracer("test"),
	}

	handler := s.observabilityMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := spanFromContext(r.Context())
		if span == nil {
			t.Error("expected span from context, got nil")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rw := httptest.NewRecorder()

	handler.ServeHTTP(rw, req)
}

func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}
