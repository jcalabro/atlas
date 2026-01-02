package pds

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleRobots(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/robots.txt", nil)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))

	body := w.Body.String()
	require.Contains(t, body, "User-agent: *")
	require.Contains(t, body, "Allow: /")
}
