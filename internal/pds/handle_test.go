package pds

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/stretchr/testify/require"
)

func TestHandleResolveHandle(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()

	dir, ok := srv.directory.(*identity.MockDirectory)
	require.True(t, ok, "directory must be a MockDirectory")

	// add test data to the mock directory
	testHandle, err := syntax.ParseHandle("alice.test")
	require.NoError(t, err)
	testDID, err := syntax.ParseDID("did:plc:abc123")
	require.NoError(t, err)
	dir.Insert(identity.Identity{
		DID:    testDID,
		Handle: testHandle,
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.identity.resolveHandle?handle=alice.test", nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
		require.JSONEq(t, `{"did":"did:plc:abc123"}`, w.Body.String())
	})

	t.Run("missing handle parameter", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.identity.resolveHandle", nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})

	t.Run("invalid handle format", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		invalidHandle := url.QueryEscape("not a valid handle!")
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.identity.resolveHandle?handle=123"+invalidHandle, nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})

	t.Run("handle not found", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.identity.resolveHandle?handle=notfound.test", nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})
}
