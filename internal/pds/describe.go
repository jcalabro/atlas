package pds

import (
	"fmt"
	"net/http"

	"github.com/bluesky-social/indigo/api/atproto"
)

func (s *server) handleDescribeServer(w http.ResponseWriter, r *http.Request) {
	host := hostFromContext(r.Context())
	if host == nil {
		s.internalErr(w, fmt.Errorf("host config not found in context"))
		return
	}

	nullStr := func(str string) *string {
		if str == "" {
			return nil
		}
		return &str
	}

	// @NOTE (jrc): we haven't implemented invite codes or phone # verification yet
	s.jsonOK(w, &atproto.ServerDescribeServer_Output{
		AvailableUserDomains: host.userDomains,
		Contact: &atproto.ServerDescribeServer_Contact{
			Email: nullStr(host.contactEmail),
		},
		Did: host.serviceDID,
		Links: &atproto.ServerDescribeServer_Links{
			PrivacyPolicy:  nullStr(host.privacyPolicy),
			TermsOfService: nullStr(host.termsOfService),
		},
	})
}
