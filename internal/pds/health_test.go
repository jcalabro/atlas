package pds

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandlePing(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/plain", w.Header().Get("Content-Type"))
	require.Equal(t, "OK", w.Body.String())
}

func TestHandleHealth(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/xrpc/_health", nil)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.JSONEq(t, `{"version":"unset"}`, w.Body.String())
}
