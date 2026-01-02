package pds

import "net/http"

func (s *server) handleRobots(w http.ResponseWriter, r *http.Request) {
	robots := `# Hello!

# Crawling the public API is allowed
User-agent: *
Allow: /
`

	s.plaintextOK(w, "%s", robots)
}
