package pds

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/google/uuid"
	"github.com/jcalabro/atlas/internal/types"
)

// createServiceAuthToken creates a service auth JWT for proxying requests.
// The token is signed with the actor's K256 signing key using ES256K.
func createServiceAuthToken(actor *types.Actor, aud, lxm string) (string, error) {
	privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
	if err != nil {
		return "", fmt.Errorf("failed to parse signing key: %w", err)
	}

	header := map[string]string{
		"alg": "ES256K",
		"typ": "JWT",
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("failed to marshal header: %w", err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(headerJSON)

	payload := map[string]any{
		"iss": actor.Did,
		"aud": aud,
		"lxm": lxm,
		"jti": uuid.NewString(),
		"exp": time.Now().Add(time.Minute).UTC().Unix(),
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payloadJSON)

	input := encodedHeader + "." + encodedPayload

	// sign using the K256 key - HashAndSign hashes with SHA256 internally
	sig, err := privkey.HashAndSign([]byte(input))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	encodedSig := base64.RawURLEncoding.EncodeToString(sig)

	// remove any trailing padding (though RawURLEncoding shouldn't add any)
	encodedSig = strings.TrimRight(encodedSig, "=")

	return input + "." + encodedSig, nil
}
