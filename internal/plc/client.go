package plc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/util"
	"go.opentelemetry.io/otel/trace"
)

// We use an interface so we can easily mock out PLC operations during tests
type PLC interface {
	CreateDID(ctx context.Context, sigkey *atcrypto.PrivateKeyK256, rotationKey atcrypto.PrivateKey, recovery string, handle string) (string, *Operation, error)
	SendOperation(ctx context.Context, did string, op *Operation) error
}

type Client struct {
	tracer trace.Tracer

	client *http.Client
	plcURL string
}

type ClientArgs struct {
	Tracer trace.Tracer

	PLCURL string
}

func NewClient(args *ClientArgs) (*Client, error) {
	return &Client{
		tracer: args.Tracer,
		client: util.RobustHTTPClient(),
		plcURL: args.PLCURL,
	}, nil
}

func (c *Client) CreateDID(
	ctx context.Context,
	sigkey *atcrypto.PrivateKeyK256,
	rotationKey atcrypto.PrivateKey,
	recovery string,
	handle string,
) (string, *Operation, error) {
	_, span := c.tracer.Start(ctx, "plc/CreateDID")
	defer span.End()

	creds, err := createDIDCredentials(sigkey, rotationKey, recovery, handle)
	if err != nil {
		return "", nil, err
	}

	op := Operation{
		Type:                "plc_operation",
		VerificationMethods: creds.VerificationMethods,
		RotationKeys:        creds.RotationKeys,
		AlsoKnownAs:         creds.AlsoKnownAs,
		Services:            creds.Services,
		Prev:                nil,
	}

	if err := signOp(rotationKey, &op); err != nil {
		return "", nil, err
	}

	did, err := DIDFromOp(&op)
	if err != nil {
		return "", nil, err
	}

	return did, &op, nil
}

func createDIDCredentials(sigkey *atcrypto.PrivateKeyK256, rotationKey atcrypto.PrivateKey, recovery, handle string) (*DIDCredentials, error) {
	// @TODO (jrc): load the list of supported PDS hostnames at startup
	parts := strings.Split(handle, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid number of handle parts")
	}
	pdsHostname := fmt.Sprintf("%s.%s", parts[len(parts)-2], parts[len(parts)-1])

	return CreateDIDCredentials(sigkey, rotationKey, recovery, handle, pdsHostname)
}

func CreateDIDCredentials(sigkey *atcrypto.PrivateKeyK256, rotationKey atcrypto.PrivateKey, recovery, handle, pdsHostname string) (*DIDCredentials, error) {
	pubsigkey, err := sigkey.PublicKey()
	if err != nil {
		return nil, err
	}

	pubrotkey, err := rotationKey.PublicKey()
	if err != nil {
		return nil, err
	}

	rotationKeys := []string{pubrotkey.DIDKey()}
	if recovery != "" {
		rotationKeys = func(recovery string) []string {
			return append([]string{recovery}, rotationKeys...)
		}(recovery)
	}

	creds := DIDCredentials{
		VerificationMethods: map[string]string{
			"atproto": pubsigkey.DIDKey(),
		},
		RotationKeys: rotationKeys,
		AlsoKnownAs:  []string{fmt.Sprintf("at://%s", handle)},
		Services: map[string]OperationService{
			"atproto_pds": {
				Type:     "AtprotoPersonalDataServer",
				Endpoint: fmt.Sprintf("https://%s", pdsHostname),
			},
		},
	}

	return &creds, nil
}

func (c *Client) SignOp(rotationKey atcrypto.PrivateKey, op *Operation) error {
	return signOp(rotationKey, op)
}

func signOp(rotationKey atcrypto.PrivateKey, op *Operation) error {
	b, err := op.MarshalCBOR()
	if err != nil {
		return err
	}

	sig, err := rotationKey.HashAndSign(b)
	if err != nil {
		return err
	}

	op.Sig = base64.RawURLEncoding.EncodeToString(sig)

	return nil
}

func (c *Client) SendOperation(ctx context.Context, did string, op *Operation) error {
	_, span := c.tracer.Start(ctx, "plc/SendOperation")
	defer span.End()

	body, err := json.Marshal(op)
	if err != nil {
		return err
	}

	u := fmt.Sprintf("%s/%s", c.plcURL, url.QueryEscape(did))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() // nolint:errcheck

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read plc operation response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"failed to submit plc genesis operation, status %d, response %q",
			resp.StatusCode,
			body,
		)
	}

	return nil
}

func DIDFromOp(op *Operation) (string, error) {
	b, err := op.MarshalCBOR()
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	b32 := strings.ToLower(base32.StdEncoding.EncodeToString(s[:]))
	return "did:plc:" + b32[0:24], nil
}
