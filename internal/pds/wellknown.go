package pds

import (
	"fmt"
	"net/http"
	"strings"
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
	host := hostFromContext(r.Context())
	if host == nil {
		s.internalErr(w, fmt.Errorf("host config not found in context"))
		return
	}

	doc := didDocument{
		Context: []string{"https://www.w3.org/ns/did/v1"},
		ID:      host.serviceDID,
		Service: []didService{
			{
				ID:              "#atproto_pds",
				Type:            "AtprotoPersonalDataServer",
				ServiceEndpoint: fmt.Sprintf("https://%s", host.hostname),
			},
		},
	}

	s.jsonOK(w, doc)
}

func (s *server) handleAtprotoDid(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	host := hostFromContext(ctx)
	if host == nil {
		s.internalErr(w, fmt.Errorf("host config not found in context"))
		return
	}

	// strip port from host header if present
	reqHost := r.Host
	if idx := strings.LastIndex(reqHost, ":"); idx != -1 {
		reqHost = reqHost[:idx]
	}

	// if the host matches our configured hostname, return the server's DID
	if reqHost == host.hostname {
		s.plaintextOK(w, "%s", host.serviceDID)
		return
	}

	// check if this is a user handle subdomain
	// user handles are like: user.pds1.dev.atlaspds.net
	// the host middleware already validated the base host, so if we got here
	// with a different reqHost, it might be a handle lookup
	actor, err := s.db.GetActorByHandle(ctx, reqHost)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to look up handle: %w", err))
		return
	}
	if actor != nil && actor.PdsHost == host.hostname {
		s.plaintextOK(w, "%s", actor.Did)
		return
	}

	// not found
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
	host := hostFromContext(r.Context())
	if host == nil {
		s.internalErr(w, fmt.Errorf("host config not found in context"))
		return
	}

	resource := oauthProtectedResource{
		Resource:               fmt.Sprintf("https://%s", host.hostname),
		AuthorizationServers:   []string{fmt.Sprintf("https://%s", host.hostname)},
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
	host := hostFromContext(r.Context())
	if host == nil {
		s.internalErr(w, fmt.Errorf("host config not found in context"))
		return
	}

	baseURL := fmt.Sprintf("https://%s", host.hostname)

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
