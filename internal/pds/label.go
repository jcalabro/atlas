package pds

import (
	"net/http"

	"github.com/bluesky-social/indigo/api/atproto"
)

func (s *server) handleQueryLabels(w http.ResponseWriter, r *http.Request) {
	// if the client requests proxying via atproto-proxy header, proxy to appview
	if r.Header.Get("atproto-proxy") != "" && s.appviewProxy != nil {
		if err := s.appviewProxy.proxy(w, r); err != nil {
			s.log.Error("proxy error", "err", err, "path", r.URL.Path)
			s.internalErr(w, err)
		}
		return
	}

	// otherwise, return empty labels
	// PDSes don't typically store labels - that's the job of labelers
	s.jsonOK(w, &atproto.LabelQueryLabels_Output{
		Labels: []*atproto.LabelDefs_Label{},
	})
}
