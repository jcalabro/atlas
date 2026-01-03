package types

import (
	"github.com/jcalabro/atlas/internal/at"
)

func (r *Record) URI() *at.URI {
	return &at.URI{
		Repo:       r.Did,
		Collection: r.Collection,
		Rkey:       r.Rkey,
	}
}
