package pds

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jcalabro/atlas/internal/env"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type spanContextKey struct{}

func spanFromContext(ctx context.Context) trace.Span {
	if span, ok := ctx.Value(spanContextKey{}).(trace.Span); ok {
		return span
	}
	return trace.SpanFromContext(ctx)
}

type responseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	size, err := rw.ResponseWriter.Write(b)
	rw.size += size
	return size, err
}

func (s *server) observabilityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := s.tracer.Start(r.Context(), r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		ctx = context.WithValue(ctx, spanContextKey{}, span)

		rw := &responseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}

		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.path", r.URL.Path),
			attribute.String("http.remote_addr", r.RemoteAddr),
			attribute.String("http.user_agent", r.UserAgent()),
		)

		s.log.Debug("incoming request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("user_agent", r.UserAgent()),
		)

		start := time.Now()
		next.ServeHTTP(rw, r.WithContext(ctx))
		duration := time.Since(start).Seconds()

		span.SetAttributes(
			attribute.Int("http.status_code", rw.status),
			attribute.Int("http.response_size", rw.size),
			attribute.Float64("http.duration_seconds", duration),
		)

		if rw.status >= 400 {
			span.SetStatus(codes.Error, http.StatusText(rw.status))
		} else {
			span.SetStatus(codes.Ok, "")
		}

		status := strconv.Itoa(rw.status)
		requests.WithLabelValues(env.Version, serviceName, r.URL.Path, r.Method, status).Inc()
		requestDuration.WithLabelValues(serviceName, r.URL.Path, r.Method, status).Observe(duration)

		s.log.Debug("request completed",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rw.status),
			slog.Int("response_size", rw.size),
			slog.Float64("duration_seconds", duration),
		)
	})
}
