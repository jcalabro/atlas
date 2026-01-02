package plc

import (
	"context"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
)

// MockClient is a mock PLC client for testing
type MockClient struct {
	CreateDIDFunc     func(ctx context.Context, sigkey *atcrypto.PrivateKeyK256, rotationKey atcrypto.PrivateKey, recovery string, handle string) (string, *Operation, error)
	SendOperationFunc func(ctx context.Context, did string, op *Operation) error
}

func (m *MockClient) SetCreateDIDFunc(fn func(ctx context.Context, sigkey *atcrypto.PrivateKeyK256, rotationKey atcrypto.PrivateKey, recovery string, handle string) (string, *Operation, error)) {
	m.CreateDIDFunc = fn
}

func (m *MockClient) SetSendOperationFunc(fn func(ctx context.Context, did string, op *Operation) error) {
	m.SendOperationFunc = fn
}

func (m *MockClient) CreateDID(ctx context.Context, sigkey *atcrypto.PrivateKeyK256, rotationKey atcrypto.PrivateKey, recovery string, handle string) (string, *Operation, error) {
	fn := m.CreateDIDFunc

	if fn != nil {
		return fn(ctx, sigkey, rotationKey, recovery, handle)
	}

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

func (m *MockClient) SendOperation(ctx context.Context, did string, op *Operation) error {
	if m.SendOperationFunc != nil {
		return m.SendOperationFunc(ctx, did, op)
	}
	// Default no-op implementation (don't actually send to PLC)
	return nil
}
