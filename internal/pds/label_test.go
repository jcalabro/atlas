package pds

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/stretchr/testify/require"
)

func TestHandleQueryLabels(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()

	t.Run("success - returns empty labels without proxy header", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.label.queryLabels?uriPatterns=at://did:plc:test/*", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var out atproto.LabelQueryLabels_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.NotNil(t, out.Labels)
		require.Empty(t, out.Labels)
		require.Nil(t, out.Cursor)
	})

	t.Run("success - returns empty labels with no parameters", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.label.queryLabels", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.LabelQueryLabels_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.NotNil(t, out.Labels)
		require.Empty(t, out.Labels)
	})

	t.Run("success - returns empty labels with sources parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.label.queryLabels?sources=did:plc:labeler", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.LabelQueryLabels_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.NotNil(t, out.Labels)
		require.Empty(t, out.Labels)
	})

	t.Run("proxies when atproto-proxy header is set and proxy is configured", func(t *testing.T) {
		t.Parallel()

		// create a mock appview server
		mockAppview := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/xrpc/com.atproto.label.queryLabels", r.URL.Path)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			resp := atproto.LabelQueryLabels_Output{
				Labels: []*atproto.LabelDefs_Label{
					{
						Src: "did:plc:labeler",
						Uri: "at://did:plc:test/app.bsky.feed.post/abc",
						Val: "test-label",
						Cts: "2024-01-01T00:00:00Z",
					},
				},
			}
			json.NewEncoder(w).Encode(resp) //nolint:errcheck
		}))
		defer mockAppview.Close()

		// create server with appview proxy configured
		srvWithProxy := testServer(t)
		srvWithProxy.appviewProxy = newAppviewProxy(srvWithProxy.log, []string{mockAppview.URL})
		routerWithProxy := srvWithProxy.router()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.label.queryLabels?uriPatterns=at://did:plc:test/*", nil)
		req.Header.Set("atproto-proxy", "did:web:labeler.example.com#atproto_labeler")
		req = addTestHostContext(srvWithProxy, req)
		routerWithProxy.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.LabelQueryLabels_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Len(t, out.Labels, 1)
		require.Equal(t, "did:plc:labeler", out.Labels[0].Src)
		require.Equal(t, "test-label", out.Labels[0].Val)
	})

	t.Run("returns empty when atproto-proxy header set but no proxy configured", func(t *testing.T) {
		t.Parallel()

		// test server has no appview proxy configured
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.label.queryLabels?uriPatterns=at://did:plc:test/*", nil)
		req.Header.Set("atproto-proxy", "did:web:labeler.example.com#atproto_labeler")
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.LabelQueryLabels_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.NotNil(t, out.Labels)
		require.Empty(t, out.Labels)
	})
}
