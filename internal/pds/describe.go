package pds

import (
	"net/http"

	"github.com/bluesky-social/indigo/api/atproto"
)

func (s *server) handleDescribeServer(w http.ResponseWriter, r *http.Request) {
	nullStr := func(str string) *string {
		if str == "" {
			return nil
		}
		return &str
	}

	// @NOTE (jrc): we haven't implemented invite codes or phone # verification yet
	s.jsonOK(w, &atproto.ServerDescribeServer_Output{
		AvailableUserDomains: s.cfg.userDomains,
		Contact: &atproto.ServerDescribeServer_Contact{
			Email: nullStr(s.cfg.contactEmail),
		},
		Did: s.cfg.serviceDID,
		Links: &atproto.ServerDescribeServer_Links{
			PrivacyPolicy:  nullStr(s.cfg.privacyPolicy),
			TermsOfService: nullStr(s.cfg.termsOfService),
		},
	})
}
