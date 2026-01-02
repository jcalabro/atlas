package pds

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/stretchr/testify/require"
)

func TestHandleDescribeServer(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.describeServer", nil)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp atproto.ServerDescribeServer_Output
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)

	// verify expected values from test server config
	require.Equal(t, "did:web:dev.atlaspds.dev", resp.Did)
	require.Equal(t, []string{".dev.atlaspds.dev"}, resp.AvailableUserDomains)
	require.NotNil(t, resp.Contact)
	require.NotNil(t, resp.Contact.Email)
	require.Equal(t, "webmaster@dev.atlaspds.dev", *resp.Contact.Email)
	require.NotNil(t, resp.Links)
	require.NotNil(t, resp.Links.PrivacyPolicy)
	require.Equal(t, "https://dev.atlaspds.dev/privacy", *resp.Links.PrivacyPolicy)
	require.NotNil(t, resp.Links.TermsOfService)
	require.Equal(t, "https://dev.atlaspds.dev/tos", *resp.Links.TermsOfService)
}
