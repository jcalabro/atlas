package at

import (
	"fmt"
	"strings"
)

type URI struct {
	Repo       string `json:"repo"`
	Collection string `json:"collection"`
	Rkey       string `json:"rkey"`
}

func (u *URI) String() string {
	return fmt.Sprintf("at://%s/%s/%s", u.Repo, u.Collection, u.Rkey)
}

// Parses an AT URI to its component parts (i.e. `at://did/collection/rkey`)
func ParseURI(uri string) (*URI, error) {
	uri = strings.TrimPrefix(uri, "at://")

	parts := strings.Split(uri, "/")
	if len(parts) < 3 {
		return nil, fmt.Errorf("not enough component parts")
	}

	u := &URI{
		Repo:       parts[0],
		Collection: parts[1],
		Rkey:       parts[2],
	}

	if u.Repo == "" {
		return nil, fmt.Errorf("repo must not be empty")
	}
	if u.Collection == "" {
		return nil, fmt.Errorf("collection must not be empty")
	}
	if u.Rkey == "" {
		return nil, fmt.Errorf("rkey must not be empty")
	}

	return u, nil
}
