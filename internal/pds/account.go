package pds

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/jcalabro/atlas/internal/metrics"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/jcalabro/atlas/internal/util"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	span := spanFromContext(r.Context())

	var in atproto.ServerCreateAccount_Input
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		s.badRequest(w, fmt.Errorf("invalid create account json: %w", err))
		return
	}

	in.Handle = strings.ToLower(in.Handle)

	span.SetAttributes(
		metrics.NilString("did", in.Did),
		metrics.NilString("email", in.Email),
		attribute.String("handle", in.Handle),
	)

	if err := validateCreateAccountInput(&in); err != nil {
		s.badRequest(w, fmt.Errorf("invalid create account payload: %w", err))
		return
	}

	handle, err := syntax.ParseHandle(in.Handle)
	if err != nil {
		s.badRequest(w, fmt.Errorf("invalid handle: %w", err))
		return
	}

	// check if the handle is already taken
	_, err = s.directory.LookupHandle(r.Context(), handle)
	if err == nil {
		s.badRequest(w, fmt.Errorf("handle %q is already taken", in.Handle))
		return
	}
	if !errors.Is(err, identity.ErrHandleNotFound) {
		s.internalErr(w, fmt.Errorf("failed to resolve handle: %w", err))
		return
	}

	// see if the email is already taken
	// @TODO (jrc): implement this

	signingKey, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to create signing key: %w", err))
		return
	}

	pwHash, err := bcrypt.GenerateFromPassword([]byte(*in.Password), bcrypt.DefaultCost)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to hash password: %w", err))
		return
	}

	// submit the genesis operation to PLC
	// @TODO (jrc)

	actor := &types.Actor{
		Did:                   util.RandString(12), // @FIXME (jrc)
		CreatedAt:             timestamppb.Now(),
		Email:                 *in.Email,
		EmailVerificationCode: fmt.Sprintf("%s-%s", util.RandString(6), util.RandString(6)),
		EmailConfirmed:        false,
		PasswordHash:          pwHash,
		SigningKey:            signingKey.Bytes(),
		Handle:                in.Handle,
		Active:                true,
	}

	if err := s.db.SaveActor(r.Context(), actor); err != nil {
		s.internalErr(w, fmt.Errorf("failed to write actor to database: %w", err))
		return
	}

	res := atproto.ServerCreateAccount_Output{
		AccessJwt: "", // @TODO (jrc)
		Did:       actor.Did,
		DidDoc:    nil, // @TODO (jrc)
		Handle:    actor.Handle,
	}

	s.jsonOK(w, res)
}

func validateCreateAccountInput(in *atproto.ServerCreateAccount_Input) error {
	switch {
	case in.Email == nil || *in.Email == "":
		return fmt.Errorf("email is required")
	case in.Handle == "":
		return fmt.Errorf("email is required")
	case in.Password == nil || *in.Password == "":
		return fmt.Errorf("password is required")
	}

	return nil
}
