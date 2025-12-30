package pds

import "net/http"

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeOK(w, `{"status":"ok"}`)
}
