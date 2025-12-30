package pds

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleHealth(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/_health", nil)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.JSONEq(t, `{"status":"ok"}`, w.Body.String())
}
