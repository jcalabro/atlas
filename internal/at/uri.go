package at

import (
	"fmt"
	"strings"
)

type URI struct {
	DID        string `json:"did"`
	Collection string `json:"collection"`
	Rkey       string `json:"rkey"`
}

func ParseURI(uri string) (URI, error) {
	var u URI

	if !strings.HasPrefix(uri, "at://") {
		return u, fmt.Errorf("invalid AT URI: must start with at://")
	}

	rest := strings.TrimPrefix(uri, "at://")
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 3 {
		return u, fmt.Errorf("invalid AT URI %q", uri)
	}

	return URI{
		DID:        parts[0],
		Collection: parts[1],
		Rkey:       parts[2],
	}, nil
}
