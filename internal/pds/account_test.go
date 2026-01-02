package pds

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/google/uuid"
	"github.com/jcalabro/atlas/internal/plc"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

type createAccountResponse struct {
	Code int
	Body *httptest.ResponseRecorder
	Out  *atproto.ServerCreateAccount_Output
}

func createAccount(t *testing.T, srv *server, input *atproto.ServerCreateAccount_Input) *createAccountResponse {
	t.Helper()

	body, err := json.Marshal(input)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createAccount", bytes.NewReader(body))
	w := httptest.NewRecorder()

	srv.handleCreateAccount(w, req)

	resp := &createAccountResponse{
		Code: w.Code,
		Body: w,
	}

	if w.Code == http.StatusOK {
		var out atproto.ServerCreateAccount_Output
		err = json.NewDecoder(w.Body).Decode(&out)
		require.NoError(t, err)
		resp.Out = &out
	}

	return resp
}

func uniqueEmail() *string {
	email := fmt.Sprintf("test-%s@example.com", uuid.NewString())
	return &email
}

func uniqueHandle() string {
	return fmt.Sprintf("test-%s.atlaspds.net", strings.ReplaceAll(uuid.NewString(), "-", ""))
}

func TestHandleCreateAccount(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	srv := testServer(t)

	t.Run("creates account successfully", func(t *testing.T) {
		t.Parallel()

		email := uniqueEmail()
		handle := uniqueHandle()
		password := "secure-password-123"

		input := atproto.ServerCreateAccount_Input{
			Email:    email,
			Handle:   handle,
			Password: &password,
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusOK, resp.Code)
		require.NotNil(t, resp.Out)

		require.NotEmpty(t, resp.Out.Did)
		require.True(t, strings.HasPrefix(resp.Out.Did, "did:plc:"))
		require.Equal(t, handle, resp.Out.Handle)
		require.NotEmpty(t, resp.Out.AccessJwt)
		require.NotEmpty(t, resp.Out.RefreshJwt)

		// verify the actor was saved to the database
		actor, err := srv.db.GetActorByEmail(ctx, *email)
		require.NoError(t, err)
		require.NotNil(t, actor)
		require.Equal(t, resp.Out.Did, actor.Did)
		require.Equal(t, *email, actor.Email)
		require.Equal(t, handle, actor.Handle)
		require.True(t, actor.Active)
		require.False(t, actor.EmailConfirmed)
		require.NotEmpty(t, actor.EmailVerificationCode)
		require.NotEmpty(t, actor.PasswordHash)
		require.NotEmpty(t, actor.SigningKey)
		require.NotEmpty(t, actor.RotationKeys)

		// verify password was hashed correctly
		err = bcrypt.CompareHashAndPassword(actor.PasswordHash, []byte(password))
		require.NoError(t, err)

		require.Len(t, actor.RefreshTokens, 1)
		require.Equal(t, resp.Out.RefreshJwt, actor.RefreshTokens[0].Token)

		// actually attempt to verify the returned tokens to make sure it's valid
		refreshClaims, err := srv.verifyRefreshToken(ctx, resp.Out.RefreshJwt)
		require.NoError(t, err)
		require.NotNil(t, refreshClaims)
		require.Equal(t, resp.Out.Did, refreshClaims.DID)

		accessClaims, err := srv.verifyAccessToken(ctx, resp.Out.AccessJwt)
		require.NoError(t, err)
		require.NotNil(t, accessClaims)
		require.Equal(t, resp.Out.Did, accessClaims.DID)
	})

	t.Run("normalizes handle to lowercase", func(t *testing.T) {
		t.Parallel()

		handle := fmt.Sprintf("TEST-%s.AtLaSPDS.net", strings.ReplaceAll(uuid.NewString(), "-", ""))
		password := "secure-password-123"

		input := atproto.ServerCreateAccount_Input{
			Email:    uniqueEmail(),
			Handle:   handle,
			Password: &password,
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusOK, resp.Code)
		require.NotNil(t, resp.Out)
		require.Equal(t, strings.ToLower(handle), resp.Out.Handle)
	})

	t.Run("rejects invalid JSON", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.server.createAccount", bytes.NewReader([]byte("invalid json")))
		w := httptest.NewRecorder()

		srv.handleCreateAccount(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), "invalid create account json")
	})

	t.Run("rejects missing email", func(t *testing.T) {
		t.Parallel()

		password := "secure-password-123"
		input := atproto.ServerCreateAccount_Input{
			Handle:   uniqueHandle(),
			Password: &password,
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusBadRequest, resp.Code)
		require.Contains(t, resp.Body.Body.String(), "email is required")
	})

	t.Run("rejects missing handle", func(t *testing.T) {
		t.Parallel()

		email := uniqueEmail()
		password := "secure-password-123"
		input := atproto.ServerCreateAccount_Input{
			Email:    email,
			Password: &password,
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusBadRequest, resp.Code)
		require.Contains(t, resp.Body.Body.String(), "handle is required")
	})

	t.Run("rejects missing password", func(t *testing.T) {
		t.Parallel()

		email := uniqueEmail()
		input := atproto.ServerCreateAccount_Input{
			Email:  email,
			Handle: uniqueHandle(),
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusBadRequest, resp.Code)
		require.Contains(t, resp.Body.Body.String(), "password is required")
	})

	t.Run("rejects short password", func(t *testing.T) {
		t.Parallel()

		email := uniqueEmail()
		password := "short"
		input := atproto.ServerCreateAccount_Input{
			Email:    email,
			Handle:   uniqueHandle(),
			Password: &password,
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusBadRequest, resp.Code)
		require.Contains(t, resp.Body.Body.String(), "password must be at least 12 characters")
	})

	t.Run("rejects invalid handle format", func(t *testing.T) {
		t.Parallel()

		email := uniqueEmail()
		password := "secure-password-123"
		input := atproto.ServerCreateAccount_Input{
			Email:    email,
			Handle:   "invalid handle with spaces",
			Password: &password,
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusBadRequest, resp.Code)
		require.Contains(t, resp.Body.Body.String(), "invalid handle")
	})

	t.Run("rejects duplicate email", func(t *testing.T) {
		t.Parallel()

		email := uniqueEmail()
		password := "secure-password-123"
		input1 := atproto.ServerCreateAccount_Input{
			Email:    email,
			Handle:   uniqueHandle(),
			Password: &password,
		}

		resp1 := createAccount(t, srv, &input1)
		require.Equal(t, http.StatusOK, resp1.Code)

		input2 := atproto.ServerCreateAccount_Input{
			Email:    email,
			Handle:   uniqueHandle(),
			Password: &password,
		}

		resp2 := createAccount(t, srv, &input2)
		require.Equal(t, http.StatusBadRequest, resp2.Code)
	})

	t.Run("rejects duplicate handle", func(t *testing.T) {
		t.Parallel()

		handle := uniqueHandle()
		password := "secure-password-123"
		input1 := atproto.ServerCreateAccount_Input{
			Email:    uniqueEmail(),
			Handle:   handle,
			Password: &password,
		}

		resp1 := createAccount(t, srv, &input1)
		require.Equal(t, http.StatusOK, resp1.Code)
		require.NotNil(t, resp1.Out)

		// insert the handle into the mock directory to simulate it being registered in PLC
		mockDir, ok := srv.directory.(*identity.MockDirectory)
		require.True(t, ok)
		did, err := syntax.ParseDID(resp1.Out.Did)
		require.NoError(t, err)
		parsedHandle, err := syntax.ParseHandle(handle)
		require.NoError(t, err)
		ident := &identity.Identity{
			DID:    did,
			Handle: parsedHandle,
		}
		mockDir.Insert(*ident)

		input2 := atproto.ServerCreateAccount_Input{
			Email:    uniqueEmail(),
			Handle:   handle,
			Password: &password,
		}

		resp2 := createAccount(t, srv, &input2)
		require.Equal(t, http.StatusBadRequest, resp2.Code)
		require.Contains(t, resp2.Body.Body.String(), "already taken")
	})

	t.Run("handles PLC CreateDID error", func(t *testing.T) {
		t.Parallel()

		srv := testServer(t)
		srv.plc = &plc.MockClient{
			CreateDIDFunc: func(ctx context.Context, sigkey *atcrypto.PrivateKeyK256, rotationKey atcrypto.PrivateKey, recovery string, handle string) (string, *plc.Operation, error) {
				return "", nil, fmt.Errorf("plc create did failed")
			},
		}

		password := "secure-password-123"
		input := atproto.ServerCreateAccount_Input{
			Email:    uniqueEmail(),
			Handle:   uniqueHandle(),
			Password: &password,
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusInternalServerError, resp.Code)
		require.Contains(t, resp.Body.Body.String(), "failed to create did")
	})

	t.Run("handles PLC SendOperation error", func(t *testing.T) {
		t.Parallel()

		srv := testServer(t)
		srv.plc = &plc.MockClient{
			SendOperationFunc: func(ctx context.Context, did string, op *plc.Operation) error {
				return fmt.Errorf("plc send operation failed")
			},
		}

		password := "secure-password-123"
		input := atproto.ServerCreateAccount_Input{
			Email:    uniqueEmail(),
			Handle:   uniqueHandle(),
			Password: &password,
		}

		resp := createAccount(t, srv, &input)
		require.Equal(t, http.StatusInternalServerError, resp.Code)
		require.Contains(t, resp.Body.Body.String(), "failed to submit plc operation")
	})
}
