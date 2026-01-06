package pds

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jcalabro/atlas/internal/env"
	"github.com/jcalabro/atlas/internal/pds/db"
	"github.com/jcalabro/atlas/internal/pds/metrics"
	"github.com/jcalabro/atlas/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type actorContextKey struct{}
type hostContextKey struct{}
type spanContextKey struct{}
type tokenContextKey struct{}

func actorFromContext(ctx context.Context) *types.Actor {
	if actor, ok := ctx.Value(actorContextKey{}).(*types.Actor); ok {
		return actor
	}
	return nil
}

func hostFromContext(ctx context.Context) *loadedHostConfig {
	if cfg, ok := ctx.Value(hostContextKey{}).(*loadedHostConfig); ok {
		return cfg
	}
	return nil
}

func spanFromContext(ctx context.Context) trace.Span {
	if span, ok := ctx.Value(spanContextKey{}).(trace.Span); ok {
		return span
	}
	return trace.SpanFromContext(ctx)
}

func tokenFromContext(ctx context.Context) string {
	if token, ok := ctx.Value(tokenContextKey{}).(string); ok {
		return token
	}
	return ""
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

// Hijack implements http.Hijacker to support WebSocket connections
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("http.ResponseWriter does not implement http.Hijacker")
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
		metrics.Requests.WithLabelValues(env.Version, serviceName, r.Host, r.URL.Path, r.Method, status).Inc()
		metrics.RequestDuration.WithLabelValues(serviceName, r.Host, r.URL.Path, r.Method, status).Observe(duration)

		s.log.Debug("request completed",
			slog.String("host", r.Host),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rw.status),
			slog.Int("response_size", rw.size),
			slog.Float64("duration_seconds", duration),
		)
	})
}

// hostMiddleware validates the Host header against configured PDS hosts
// and stores the host configuration in the request context.
func (s *server) hostMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Host

		// strip port if present
		if idx := strings.LastIndex(header, ":"); idx != -1 {
			header = header[:idx]
		}

		host := s.getHost(header)
		if host == nil {
			s.notFound(w, fmt.Errorf("host %q not found", header))
			return
		}

		ctx := context.WithValue(r.Context(), hostContextKey{}, host)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authMiddleware extracts and verifies the JWT from the Authorization header
// and loads the associated actor. For refresh endpoints, it requires a refresh token.
func (s *server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			s.unauthorized(w, fmt.Errorf("authorization header is required"))
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			s.unauthorized(w, fmt.Errorf("invalid authorization header format"))
			return
		}

		tokenString := parts[1]
		isRefresh := strings.HasSuffix(r.URL.Path, "refreshSession")

		var claims *VerifiedClaims
		var err error
		if isRefresh {
			claims, err = s.verifyRefreshToken(ctx, tokenString)
		} else {
			claims, err = s.verifyAccessToken(ctx, tokenString)
		}
		if err != nil {
			s.unauthorized(w, fmt.Errorf("invalid or expired token"))
			return
		}

		actor, err := s.db.GetActorByDID(ctx, claims.DID)
		if errors.Is(err, db.ErrNotFound) {
			s.unauthorized(w, fmt.Errorf("actor not found"))
			return
		}
		if err != nil {
			s.log.Error("failed to get actor by DID", "did", claims.DID, "error", err)
			s.internalErr(w, fmt.Errorf("failed to authenticate"))
			return
		}

		// verify the actor belongs to the requested PDS host
		host := hostFromContext(ctx)
		if actor.PdsHost != host.hostname {
			s.log.Debug("actor pds_host mismatch", "actor_host", actor.PdsHost, "request_host", host.hostname)
			s.unauthorized(w, fmt.Errorf("actor not found on this host"))
			return
		}

		// for refresh token requests, verify the token exists in the actor's refresh tokens
		if isRefresh {
			found := false
			for _, rt := range actor.RefreshTokens {
				if rt.Token == tokenString {
					// check if expired
					if rt.ExpiresAt.AsTime().Before(time.Now()) {
						s.unauthorized(w, fmt.Errorf("refresh token expired"))
						return
					}

					found = true
					break
				}
			}
			if !found {
				s.unauthorized(w, fmt.Errorf("refresh token not found"))
				return
			}
		}

		ctx = context.WithValue(ctx, actorContextKey{}, actor)
		ctx = context.WithValue(ctx, tokenContextKey{}, tokenString)

		next(w, r.WithContext(ctx))
	}
}
