package pds

import (
	"fmt"
	"net/http"
)

type didDocument struct {
	Context []string     `json:"@context"`
	ID      string       `json:"id"`
	Service []didService `json:"service"`
}

type didService struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

func (s *server) handleWellKnown(w http.ResponseWriter, r *http.Request) {
	doc := didDocument{
		Context: []string{"https://www.w3.org/ns/did/v1"},
		ID:      s.cfg.serviceDID,
		Service: []didService{
			{
				ID:              "#atproto_pds",
				Type:            "AtprotoPersonalDataServer",
				ServiceEndpoint: fmt.Sprintf("https://%s", s.cfg.hostname),
			},
		},
	}

	s.jsonOK(w, doc)
}

func (s *server) handleAtprotoDid(w http.ResponseWriter, r *http.Request) {
	host := r.Host

	// if the host matches our configured hostname, return the server's DID
	if host == s.cfg.hostname {
		s.plaintextOK(w, "%s", s.cfg.serviceDID)
		return
	}

	// @TODO (jrc): implement subdomain lookup for user DIDs
	// for now, return 204 No Content for any subdomain requests
	w.WriteHeader(http.StatusNoContent)
}

type oauthProtectedResource struct {
	Resource               string   `json:"resource"`
	AuthorizationServers   []string `json:"authorization_servers"`
	ScopesSupported        []string `json:"scopes_supported"`
	BearerMethodsSupported []string `json:"bearer_methods_supported"`
	ResourceDocumentation  string   `json:"resource_documentation"`
}

func (s *server) handleOauthProtectedResource(w http.ResponseWriter, r *http.Request) {
	resource := oauthProtectedResource{
		Resource:               fmt.Sprintf("https://%s", s.cfg.hostname),
		AuthorizationServers:   []string{fmt.Sprintf("https://%s", s.cfg.hostname)},
		ScopesSupported:        []string{},
		BearerMethodsSupported: []string{"header"},
		ResourceDocumentation:  "https://atproto.com",
	}

	s.jsonOK(w, resource)
}

type oauthAuthorizationServer struct {
	Issuer                                             string   `json:"issuer"`
	AuthorizationEndpoint                              string   `json:"authorization_endpoint"`
	TokenEndpoint                                      string   `json:"token_endpoint"`
	RevocationEndpoint                                 string   `json:"revocation_endpoint"`
	IntrospectionEndpoint                              string   `json:"introspection_endpoint"`
	PushedAuthorizationRequestEndpoint                 string   `json:"pushed_authorization_request_endpoint"`
	JWKSURI                                            string   `json:"jwks_uri"`
	ScopesSupported                                    []string `json:"scopes_supported"`
	SubjectTypesSupported                              []string `json:"subject_types_supported"`
	ResponseTypesSupported                             []string `json:"response_types_supported"`
	ResponseModesSupported                             []string `json:"response_modes_supported"`
	GrantTypesSupported                                []string `json:"grant_types_supported"`
	CodeChallengeMethodsSupported                      []string `json:"code_challenge_methods_supported"`
	TokenEndpointAuthMethodsSupported                  []string `json:"token_endpoint_auth_methods_supported"`
	TokenEndpointAuthSigningAlgValuesSupported         []string `json:"token_endpoint_auth_signing_alg_values_supported"`
	RevocationEndpointAuthMethodsSupported             []string `json:"revocation_endpoint_auth_methods_supported"`
	RevocationEndpointAuthSigningAlgValuesSupported    []string `json:"revocation_endpoint_auth_signing_alg_values_supported"`
	IntrospectionEndpointAuthMethodsSupported          []string `json:"introspection_endpoint_auth_methods_supported"`
	IntrospectionEndpointAuthSigningAlgValuesSupported []string `json:"introspection_endpoint_auth_signing_alg_values_supported"`
	AuthorizationResponseIssParameterSupported         bool     `json:"authorization_response_iss_parameter_supported"`
	RequirePushedAuthorizationRequests                 bool     `json:"require_pushed_authorization_requests"`
	DPoPSigningAlgValuesSupported                      []string `json:"dpop_signing_alg_values_supported"`
	ClientIDMetadataDocumentSupported                  bool     `json:"client_id_metadata_document_supported"`
	RequireSignedRequestObject                         bool     `json:"require_signed_request_object"`
}

func (s *server) handleOauthAuthorizationServer(w http.ResponseWriter, r *http.Request) {
	baseURL := fmt.Sprintf("https://%s", s.cfg.hostname)

	metadata := oauthAuthorizationServer{
		Issuer:                                             baseURL,
		AuthorizationEndpoint:                              fmt.Sprintf("%s/oauth/authorize", baseURL),
		TokenEndpoint:                                      fmt.Sprintf("%s/oauth/token", baseURL),
		RevocationEndpoint:                                 fmt.Sprintf("%s/oauth/revoke", baseURL),
		IntrospectionEndpoint:                              fmt.Sprintf("%s/oauth/introspect", baseURL),
		PushedAuthorizationRequestEndpoint:                 fmt.Sprintf("%s/oauth/par", baseURL),
		JWKSURI:                                            fmt.Sprintf("%s/oauth/jwks", baseURL),
		ScopesSupported:                                    []string{"atproto", "transition:email", "transition:generic", "transition:chat.bsky"},
		SubjectTypesSupported:                              []string{"public"},
		ResponseTypesSupported:                             []string{"code"},
		ResponseModesSupported:                             []string{"query", "fragment", "form_post"},
		GrantTypesSupported:                                []string{"authorization_code", "refresh_token"},
		CodeChallengeMethodsSupported:                      []string{"S256"},
		TokenEndpointAuthMethodsSupported:                  []string{"none", "private_key_jwt"},
		TokenEndpointAuthSigningAlgValuesSupported:         []string{"ES256"},
		RevocationEndpointAuthMethodsSupported:             []string{"none"},
		RevocationEndpointAuthSigningAlgValuesSupported:    []string{},
		IntrospectionEndpointAuthMethodsSupported:          []string{"none"},
		IntrospectionEndpointAuthSigningAlgValuesSupported: []string{},
		AuthorizationResponseIssParameterSupported:         true,
		RequirePushedAuthorizationRequests:                 true,
		DPoPSigningAlgValuesSupported:                      []string{"ES256"},
		ClientIDMetadataDocumentSupported:                  true,
		RequireSignedRequestObject:                         false,
	}

	s.jsonOK(w, metadata)
}
