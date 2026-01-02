package plc

import (
	"encoding/json"

	"github.com/bluesky-social/indigo/atproto/atdata"
	typegen "github.com/whyrusleeping/cbor-gen"
)

type DIDCredentials struct {
	VerificationMethods map[string]string           `json:"verificationMethods"`
	RotationKeys        []string                    `json:"rotationKeys"`
	AlsoKnownAs         []string                    `json:"alsoKnownAs"`
	Services            map[string]OperationService `json:"services"`
}

type Operation struct {
	Type                string                      `json:"type"`
	VerificationMethods map[string]string           `json:"verificationMethods"`
	RotationKeys        []string                    `json:"rotationKeys"`
	AlsoKnownAs         []string                    `json:"alsoKnownAs"`
	Services            map[string]OperationService `json:"services"`
	Prev                *string                     `json:"prev"`
	Sig                 string                      `json:"sig,omitempty"`
}

type OperationService struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

func (po *Operation) MarshalCBOR() ([]byte, error) {
	if po == nil {
		return typegen.CborNull, nil
	}

	b, err := json.Marshal(po)
	if err != nil {
		return nil, err
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	b, err = atdata.MarshalCBOR(m)
	if err != nil {
		return nil, err
	}

	return b, nil
}
