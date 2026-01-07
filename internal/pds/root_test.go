package pds

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleRoot(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()

	t.Run("success - returns ASCII art banner", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))

		body := w.Body.String()
		require.Contains(t, body, "AT Protocol Personal Data Server")
		require.Contains(t, body, "github.com/jcalabro/atlas")
	})
}
