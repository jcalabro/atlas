package pds

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewAppviewProxy(t *testing.T) {
	t.Parallel()
	log := slog.Default()

	t.Run("nil when no urls provided", func(t *testing.T) {
		t.Parallel()
		proxy := newAppviewProxy(log, nil)
		require.Nil(t, proxy)

		proxy = newAppviewProxy(log, []string{})
		require.Nil(t, proxy)
	})

	t.Run("creates proxy with backends", func(t *testing.T) {
		t.Parallel()
		urls := []string{"http://localhost:8001", "http://localhost:8002"}
		proxy := newAppviewProxy(log, urls)

		require.NotNil(t, proxy)
		require.Len(t, proxy.backends, 2)
		require.Equal(t, "http://localhost:8001", proxy.backends[0].url)
		require.Equal(t, "http://localhost:8002", proxy.backends[1].url)

		// backends should be initially healthy
		require.True(t, proxy.backends[0].healthy.Load())
		require.True(t, proxy.backends[1].healthy.Load())
	})
}

func TestAppviewProxyGetHealthyBackend(t *testing.T) {
	t.Parallel()
	log := slog.Default()

	t.Run("returns first healthy backend", func(t *testing.T) {
		t.Parallel()
		urls := []string{"http://first:8001", "http://second:8002"}
		proxy := newAppviewProxy(log, urls)

		// both healthy - should return first
		url, err := proxy.getHealthyBackend()
		require.NoError(t, err)
		require.Equal(t, "http://first:8001", url)

		// first unhealthy - should return second
		proxy.backends[0].healthy.Store(false)
		url, err = proxy.getHealthyBackend()
		require.NoError(t, err)
		require.Equal(t, "http://second:8002", url)
	})

	t.Run("falls back to first backend when none are healthy", func(t *testing.T) {
		t.Parallel()
		urls := []string{"http://first:8001", "http://second:8002"}
		proxy := newAppviewProxy(log, urls)

		proxy.backends[0].healthy.Store(false)
		proxy.backends[1].healthy.Store(false)

		// should still return the first backend as fallback
		url, err := proxy.getHealthyBackend()
		require.NoError(t, err)
		require.Equal(t, "http://first:8001", url)
	})

	t.Run("returns error for nil proxy", func(t *testing.T) {
		t.Parallel()
		var proxy *appviewProxy
		_, err := proxy.getHealthyBackend()
		require.Error(t, err)
	})
}

func TestAppviewProxyHealthCheck(t *testing.T) {
	t.Parallel()
	log := slog.Default()

	t.Run("marks backend healthy on 200 response", func(t *testing.T) {
		t.Parallel()

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/xrpc/_health", r.URL.Path)
			w.WriteHeader(http.StatusOK)
		}))

		proxy := newAppviewProxy(log, []string{srv.URL})
		t.Cleanup(func() {
			proxy.CloseIdleConnections()
			srv.Close()
		})
		proxy.backends[0].healthy.Store(false) // start unhealthy

		ctx := t.Context()
		proxy.checkHealth(ctx, proxy.backends[0])

		require.True(t, proxy.backends[0].healthy.Load())
	})

	t.Run("marks backend unhealthy on non-200 response", func(t *testing.T) {
		t.Parallel()

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))

		proxy := newAppviewProxy(log, []string{srv.URL})
		t.Cleanup(func() {
			proxy.CloseIdleConnections()
			srv.Close()
		})

		ctx := t.Context()
		proxy.checkHealth(ctx, proxy.backends[0])

		require.False(t, proxy.backends[0].healthy.Load())
	})

	t.Run("marks backend unhealthy on connection error", func(t *testing.T) {
		t.Parallel()

		// use an address that will fail to connect
		proxy := newAppviewProxy(log, []string{"http://127.0.0.1:1"})

		ctx := t.Context()
		proxy.checkHealth(ctx, proxy.backends[0])

		require.False(t, proxy.backends[0].healthy.Load())
	})
}

func TestAppviewProxyStart(t *testing.T) {
	t.Parallel()
	log := slog.Default()

	t.Run("health checks run periodically", func(t *testing.T) {
		t.Parallel()

		var healthCheckCount atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			healthCheckCount.Add(1)
			w.WriteHeader(http.StatusOK)
		}))

		proxy := newAppviewProxy(log, []string{srv.URL})
		t.Cleanup(func() {
			proxy.CloseIdleConnections()
			srv.Close()
		})
		proxy.healthCheckInterval = 50 * time.Millisecond

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()

		go proxy.Start(ctx)

		// wait for context to be cancelled
		<-ctx.Done()

		// should have had at least 2 health checks (initial + at least 1 periodic)
		require.GreaterOrEqual(t, healthCheckCount.Load(), int32(2))
	})

	t.Run("start is no-op for nil proxy", func(t *testing.T) {
		t.Parallel()
		var proxy *appviewProxy
		proxy.Start(t.Context()) // should not panic
	})
}

func TestAppviewProxyProxyRequest(t *testing.T) {
	t.Parallel()
	log := slog.Default()

	t.Run("proxies GET request to healthy backend", func(t *testing.T) {
		t.Parallel()

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "/xrpc/app.bsky.feed.getTimeline", r.URL.Path)
			require.Equal(t, "limit=50", r.URL.RawQuery)
			require.Equal(t, "test-value", r.Header.Get("X-Custom-Header"))

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"feed":[]}`)) // nolint:errcheck
		}))

		proxy := newAppviewProxy(log, []string{backend.URL})
		t.Cleanup(func() {
			proxy.CloseIdleConnections()
			backend.Close()
		})

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getTimeline?limit=50", nil)
		req.Header.Set("X-Custom-Header", "test-value")
		w := httptest.NewRecorder()

		err := proxy.proxy(w, req)
		require.NoError(t, err)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var resp map[string]any
		err = json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		require.NotNil(t, resp["feed"])
	})

	t.Run("proxies POST request with body", func(t *testing.T) {
		t.Parallel()

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)

			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, `{"test":"data"}`, string(body))

			w.WriteHeader(http.StatusOK)
		}))

		proxy := newAppviewProxy(log, []string{backend.URL})
		t.Cleanup(func() {
			proxy.CloseIdleConnections()
			backend.Close()
		})

		req := httptest.NewRequest(http.MethodPost, "/xrpc/some.endpoint", nil)
		req.Body = io.NopCloser(newStringReader(`{"test":"data"}`))
		w := httptest.NewRecorder()

		err := proxy.proxy(w, req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("forwards error status codes", func(t *testing.T) {
		t.Parallel()

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`)) // nolint:errcheck
		}))

		proxy := newAppviewProxy(log, []string{backend.URL})
		t.Cleanup(func() {
			proxy.CloseIdleConnections()
			backend.Close()
		})

		req := httptest.NewRequest(http.MethodGet, "/xrpc/missing.endpoint", nil)
		w := httptest.NewRecorder()

		err := proxy.proxy(w, req)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("falls back to first backend when none are healthy", func(t *testing.T) {
		t.Parallel()

		// create a backend that will receive the request even though marked unhealthy
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"fallback":true}`)) // nolint:errcheck
		}))

		proxy := newAppviewProxy(log, []string{backend.URL})
		t.Cleanup(func() {
			proxy.CloseIdleConnections()
			backend.Close()
		})
		proxy.backends[0].healthy.Store(false)

		req := httptest.NewRequest(http.MethodGet, "/xrpc/some.endpoint", nil)
		w := httptest.NewRecorder()

		err := proxy.proxy(w, req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "fallback")
	})

	t.Run("fails over to second backend when first is unhealthy", func(t *testing.T) {
		t.Parallel()

		backend2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"from":"backend2"}`)) // nolint:errcheck
		}))

		// first backend is invalid/down
		proxy := newAppviewProxy(log, []string{"http://localhost:1", backend2.URL})
		t.Cleanup(func() {
			proxy.CloseIdleConnections()
			backend2.Close()
		})
		proxy.backends[0].healthy.Store(false) // mark first as unhealthy

		req := httptest.NewRequest(http.MethodGet, "/xrpc/some.endpoint", nil)
		w := httptest.NewRecorder()

		err := proxy.proxy(w, req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "backend2")
	})
}

func TestHandleProxy(t *testing.T) {
	t.Parallel()
	srv := testServer(t)

	t.Run("returns 404 when no appview configured", func(t *testing.T) {
		t.Parallel()
		// srv.appviewProxy is nil by default

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getTimeline", nil)
		req = addTestHostContext(srv, req)

		srv.handleProxy(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("proxies request when appview configured", func(t *testing.T) {
		t.Parallel()

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"success":true}`)) // nolint:errcheck
		}))

		// create a new server with appview configured
		srvWithProxy := testServer(t)
		srvWithProxy.appviewProxy = newAppviewProxy(srvWithProxy.log, []string{backend.URL})
		t.Cleanup(func() {
			srvWithProxy.appviewProxy.CloseIdleConnections()
			backend.Close()
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getTimeline", nil)
		req = addTestHostContext(srvWithProxy, req)

		srvWithProxy.handleProxy(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), "success")
	})

	t.Run("creates service auth token when user is authenticated with atproto-proxy header", func(t *testing.T) {
		t.Parallel()

		var receivedAuthHeader string
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuthHeader = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		}))

		srvWithProxy := testServer(t)
		srvWithProxy.appviewProxy = newAppviewProxy(srvWithProxy.log, []string{backend.URL})
		t.Cleanup(func() {
			srvWithProxy.appviewProxy.CloseIdleConnections()
			backend.Close()
		})

		did := "did:plc:" + uuid.NewString()[:24]
		actor, session := setupTestActor(t, srvWithProxy, did, "proxy-test@dev.atlaspds.dev", "proxy-test.dev.atlaspds.dev")

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.notification.listNotifications", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req.Header.Set("atproto-proxy", "did:web:api.bsky.app#bsky_appview")
		req = addTestHostContext(srvWithProxy, req)

		srvWithProxy.handleProxy(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// should have a service auth token, not the original access token
		require.NotEmpty(t, receivedAuthHeader)
		require.True(t, strings.HasPrefix(receivedAuthHeader, "Bearer "))
		require.NotEqual(t, "Bearer "+session.AccessToken, receivedAuthHeader)

		// verify the service auth token has the correct structure (ES256K JWT)
		parts := strings.Split(strings.TrimPrefix(receivedAuthHeader, "Bearer "), ".")
		require.Len(t, parts, 3, "service auth token should be a JWT with 3 parts")

		// decode payload and verify claims
		payload := decodeJWTPayload(t, parts[1])
		require.Equal(t, actor.Did, payload["iss"], "issuer should be the actor's DID")
		require.Equal(t, "did:web:api.bsky.app", payload["aud"], "audience should be the service DID")
		require.Equal(t, "app.bsky.notification.listNotifications", payload["lxm"], "lxm should be the lexicon method")
	})

	t.Run("removes Authorization header when user not authenticated with atproto-proxy header", func(t *testing.T) {
		t.Parallel()

		var receivedAuthHeader string
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuthHeader = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		}))

		srvWithProxy := testServer(t)
		srvWithProxy.appviewProxy = newAppviewProxy(srvWithProxy.log, []string{backend.URL})
		t.Cleanup(func() {
			srvWithProxy.appviewProxy.CloseIdleConnections()
			backend.Close()
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.notification.listNotifications", nil)
		req.Header.Set("Authorization", "Bearer some-invalid-token")
		req.Header.Set("atproto-proxy", "did:web:api.bsky.app#bsky_appview")
		req = addTestHostContext(srvWithProxy, req)

		srvWithProxy.handleProxy(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// Authorization header should be removed since user couldn't be authenticated
		require.Empty(t, receivedAuthHeader)
	})

	t.Run("strips auth header when no atproto-proxy header", func(t *testing.T) {
		t.Parallel()

		var receivedAuthHeader string
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuthHeader = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		}))

		srvWithProxy := testServer(t)
		srvWithProxy.appviewProxy = newAppviewProxy(srvWithProxy.log, []string{backend.URL})
		t.Cleanup(func() {
			srvWithProxy.appviewProxy.CloseIdleConnections()
			backend.Close()
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getTimeline", nil)
		req.Header.Set("Authorization", "Bearer some-token")
		req = addTestHostContext(srvWithProxy, req)

		srvWithProxy.handleProxy(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// without atproto-proxy header, auth should be stripped for security
		// (we should never forward user PDS tokens to upstream services)
		require.Empty(t, receivedAuthHeader)
	})

	t.Run("returns bad request for invalid atproto-proxy header format", func(t *testing.T) {
		t.Parallel()

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		srvWithProxy := testServer(t)
		srvWithProxy.appviewProxy = newAppviewProxy(srvWithProxy.log, []string{backend.URL})
		t.Cleanup(func() {
			srvWithProxy.appviewProxy.CloseIdleConnections()
			backend.Close()
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getTimeline", nil)
		req.Header.Set("atproto-proxy", "did:web:api.bsky.app") // missing #service_id
		req = addTestHostContext(srvWithProxy, req)

		srvWithProxy.handleProxy(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})
}

// stringReader is a simple io.Reader that reads from a string
type stringReader struct {
	s string
	i int
}

func newStringReader(s string) *stringReader {
	return &stringReader{s: s}
}

func (r *stringReader) Read(p []byte) (n int, err error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n = copy(p, r.s[r.i:])
	r.i += n
	return
}
