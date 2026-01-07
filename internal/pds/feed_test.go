package pds

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleGetFeed(t *testing.T) {
	t.Parallel()

	t.Run("missing feed parameter returns 400", func(t *testing.T) {
		t.Parallel()
		srv := testServer(t)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeed", nil)
		req = addTestHostContext(srv, req)

		srv.handleGetFeed(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)

		var resp map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		require.Contains(t, resp["message"], "feed parameter is required")
	})

	t.Run("invalid feed URI returns 400", func(t *testing.T) {
		t.Parallel()
		srv := testServer(t)

		testCases := []struct {
			name string
			feed string
		}{
			{"empty URI", "at://"},
			{"missing parts", "at://did:plc:abc"},
			{"not AT URI", "https://example.com/feed"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				w := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeed?feed="+tc.feed, nil)
				req = addTestHostContext(srv, req)

				srv.handleGetFeed(w, req)

				require.Equal(t, http.StatusBadRequest, w.Code)

				var resp map[string]string
				err := json.Unmarshal(w.Body.Bytes(), &resp)
				require.NoError(t, err)
				require.Contains(t, resp["message"], "invalid feed URI")
			})
		}
	})

	t.Run("returns 404 when no appview configured", func(t *testing.T) {
		t.Parallel()
		srv := testServer(t)
		// srv.appviewProxy is nil by default

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeed?feed=at://did:plc:abc/app.bsky.feed.generator/whats-hot", nil)
		req = addTestHostContext(srv, req)

		srv.handleGetFeed(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)

		var resp map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		require.Contains(t, resp["message"], "no appview configured")
	})

	t.Run("proxies unauthenticated request to appview", func(t *testing.T) {
		t.Parallel()

		feedGenDID := "did:web:feed-generator.example.com"
		var receivedAuthHeader string

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// handle getRecord for feed generator
			if r.URL.Path == "/xrpc/com.atproto.repo.getRecord" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				resp := map[string]any{
					"uri": "at://did:plc:abc/app.bsky.feed.generator/whats-hot",
					"value": map[string]any{
						"$type":       "app.bsky.feed.generator",
						"did":         feedGenDID,
						"displayName": "What's Hot",
						"createdAt":   "2024-01-01T00:00:00Z",
					},
				}
				json.NewEncoder(w).Encode(resp) // nolint:errcheck
				return
			}

			// handle getFeed
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "/xrpc/app.bsky.feed.getFeed", r.URL.Path)
			require.Equal(t, "at://did:plc:abc/app.bsky.feed.generator/whats-hot", r.URL.Query().Get("feed"))

			receivedAuthHeader = r.Header.Get("Authorization")

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"feed":[]}`)) // nolint:errcheck
		}))
		defer backend.Close()

		srv := testServer(t)
		srv.appviewProxy = newAppviewProxy(slog.Default(), []string{backend.URL})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeed?feed=at://did:plc:abc/app.bsky.feed.generator/whats-hot", nil)
		req = addTestHostContext(srv, req)

		srv.handleGetFeed(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// unauthenticated request should have no Authorization header
		require.Empty(t, receivedAuthHeader)

		var resp map[string]any
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		require.NotNil(t, resp["feed"])
	})

	t.Run("proxies authenticated request with service auth token", func(t *testing.T) {
		t.Parallel()

		feedGenDID := "did:web:feed-generator.example.com"
		var receivedAuthHeader string

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// handle getRecord for feed generator
			if r.URL.Path == "/xrpc/com.atproto.repo.getRecord" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				resp := map[string]any{
					"uri": "at://did:plc:abc/app.bsky.feed.generator/whats-hot",
					"value": map[string]any{
						"$type":       "app.bsky.feed.generator",
						"did":         feedGenDID,
						"displayName": "What's Hot",
						"createdAt":   "2024-01-01T00:00:00Z",
					},
				}
				json.NewEncoder(w).Encode(resp) // nolint:errcheck
				return
			}

			// handle getFeed
			receivedAuthHeader = r.Header.Get("Authorization")

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"feed":[]}`)) // nolint:errcheck
		}))
		defer backend.Close()

		srv := testServer(t)
		srv.appviewProxy = newAppviewProxy(slog.Default(), []string{backend.URL})

		// set up authenticated actor
		actor, session := setupTestActor(t, srv, "did:plc:feedtest123", "feedtest@dev.atlaspds.dev", "feedtest.dev.atlaspds.dev")

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeed?feed=at://did:plc:abc/app.bsky.feed.generator/whats-hot", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addTestHostContext(srv, req)

		srv.handleGetFeed(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// authenticated request should have a service auth token (different from the original access token)
		require.NotEmpty(t, receivedAuthHeader)
		require.True(t, len(receivedAuthHeader) > 7) // "Bearer " prefix
		serviceToken := receivedAuthHeader[7:]

		// service token should be different from the original access token
		require.NotEqual(t, session.AccessToken, serviceToken)

		// verify the service token is a JWT with the expected claims
		parts := splitJWT(serviceToken)
		require.Len(t, parts, 3)

		claims := decodeJWTPayload(t, parts[1])
		require.Equal(t, actor.Did, claims["iss"])
		require.Equal(t, feedGenDID, claims["aud"])
		require.Equal(t, "app.bsky.feed.getFeedSkeleton", claims["lxm"])
		require.NotEmpty(t, claims["jti"])
		require.NotEmpty(t, claims["exp"])
	})

	t.Run("returns 404 when feed generator not found", func(t *testing.T) {
		t.Parallel()

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// return 404 for getRecord
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"RecordNotFound","message":"Record not found"}`)) // nolint:errcheck
		}))
		defer backend.Close()

		srv := testServer(t)
		srv.appviewProxy = newAppviewProxy(slog.Default(), []string{backend.URL})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeed?feed=at://did:plc:abc/app.bsky.feed.generator/nonexistent", nil)
		req = addTestHostContext(srv, req)

		srv.handleGetFeed(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
		require.Contains(t, w.Body.String(), "feed not found")
	})

	t.Run("forwards error from appview", func(t *testing.T) {
		t.Parallel()

		feedGenDID := "did:web:feed-generator.example.com"

		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// handle getRecord for feed generator
			if r.URL.Path == "/xrpc/com.atproto.repo.getRecord" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				resp := map[string]any{
					"uri": "at://did:plc:abc/app.bsky.feed.generator/broken",
					"value": map[string]any{
						"$type":       "app.bsky.feed.generator",
						"did":         feedGenDID,
						"displayName": "Broken Feed",
						"createdAt":   "2024-01-01T00:00:00Z",
					},
				}
				json.NewEncoder(w).Encode(resp) // nolint:errcheck
				return
			}

			// return error for getFeed
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"InternalServerError","message":"Feed generator unavailable"}`)) // nolint:errcheck
		}))
		defer backend.Close()

		srv := testServer(t)
		srv.appviewProxy = newAppviewProxy(slog.Default(), []string{backend.URL})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.feed.getFeed?feed=at://did:plc:abc/app.bsky.feed.generator/broken", nil)
		req = addTestHostContext(srv, req)

		srv.handleGetFeed(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
		require.Contains(t, w.Body.String(), "Feed generator unavailable")
	})
}

func TestCreateServiceAuthToken(t *testing.T) {
	t.Parallel()

	t.Run("creates valid JWT", func(t *testing.T) {
		t.Parallel()
		srv := testServer(t)

		actor, _ := setupTestActor(t, srv, "did:plc:serviceauthtest", "serviceauth@dev.atlaspds.dev", "serviceauth.dev.atlaspds.dev")

		token, err := createServiceAuthToken(actor, "did:web:target.example.com", "app.bsky.feed.getFeedSkeleton")
		require.NoError(t, err)
		require.NotEmpty(t, token)

		// verify it's a valid JWT structure
		parts := splitJWT(token)
		require.Len(t, parts, 3)

		// verify header
		header := decodeJWTPayload(t, parts[0])
		require.Equal(t, "ES256K", header["alg"])
		require.Equal(t, "JWT", header["typ"])

		// verify payload
		claims := decodeJWTPayload(t, parts[1])
		require.Equal(t, actor.Did, claims["iss"])
		require.Equal(t, "did:web:target.example.com", claims["aud"])
		require.Equal(t, "app.bsky.feed.getFeedSkeleton", claims["lxm"])
		require.NotEmpty(t, claims["jti"])
		require.NotEmpty(t, claims["exp"])
	})
}

// helper functions for JWT testing
func splitJWT(token string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(token); i++ {
		if token[i] == '.' {
			parts = append(parts, token[start:i])
			start = i + 1
		}
	}
	parts = append(parts, token[start:])
	return parts
}

func decodeJWTPayload(t *testing.T, encoded string) map[string]any {
	t.Helper()

	// RawURLEncoding doesn't need padding
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal(decoded, &result)
	require.NoError(t, err)

	return result
}
