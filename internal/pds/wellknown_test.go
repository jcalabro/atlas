package pds

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleWellKnown(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/.well-known/did.json", nil)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp didDocument
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)

	require.Equal(t, []string{"https://www.w3.org/ns/did/v1"}, resp.Context)
	require.Equal(t, "did:web:dev.atlaspds.dev", resp.ID)
	require.Len(t, resp.Service, 1)
	require.Equal(t, "#atproto_pds", resp.Service[0].ID)
	require.Equal(t, "AtprotoPersonalDataServer", resp.Service[0].Type)
	require.Equal(t, "https://dev.atlaspds.dev", resp.Service[0].ServiceEndpoint)
}

func TestHandleAtprotoDid_ServerHostname(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/.well-known/atproto-did", nil)
	req.Host = "dev.atlaspds.dev"
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))
	require.Equal(t, "did:web:dev.atlaspds.dev", w.Body.String())
}

func TestHandleAtprotoDid_InvalidSubdomain(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/.well-known/atproto-did", nil)
	req.Host = "other.example.com"
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleOauthProtectedResource(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-protected-resource", nil)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp oauthProtectedResource
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)

	require.Equal(t, "https://dev.atlaspds.dev", resp.Resource)
	require.Equal(t, []string{"https://dev.atlaspds.dev"}, resp.AuthorizationServers)
	require.Equal(t, []string{}, resp.ScopesSupported)
	require.Equal(t, []string{"header"}, resp.BearerMethodsSupported)
	require.Equal(t, "https://atproto.com", resp.ResourceDocumentation)
}

func TestHandleOauthAuthorizationServer(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	router := testServer(t).router()

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp oauthAuthorizationServer
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)

	require.Equal(t, "https://dev.atlaspds.dev", resp.Issuer)
	require.Equal(t, "https://dev.atlaspds.dev/oauth/authorize", resp.AuthorizationEndpoint)
	require.Equal(t, "https://dev.atlaspds.dev/oauth/token", resp.TokenEndpoint)
	require.Equal(t, "https://dev.atlaspds.dev/oauth/revoke", resp.RevocationEndpoint)
	require.Equal(t, "https://dev.atlaspds.dev/oauth/introspect", resp.IntrospectionEndpoint)
	require.Equal(t, "https://dev.atlaspds.dev/oauth/par", resp.PushedAuthorizationRequestEndpoint)
	require.Equal(t, "https://dev.atlaspds.dev/oauth/jwks", resp.JWKSURI)
	require.Equal(t, []string{"atproto", "transition:email", "transition:generic", "transition:chat.bsky"}, resp.ScopesSupported)
	require.Equal(t, []string{"public"}, resp.SubjectTypesSupported)
	require.Equal(t, []string{"code"}, resp.ResponseTypesSupported)
	require.Equal(t, []string{"query", "fragment", "form_post"}, resp.ResponseModesSupported)
	require.Equal(t, []string{"authorization_code", "refresh_token"}, resp.GrantTypesSupported)
	require.Equal(t, []string{"S256"}, resp.CodeChallengeMethodsSupported)
	require.True(t, resp.AuthorizationResponseIssParameterSupported)
	require.True(t, resp.RequirePushedAuthorizationRequests)
}
