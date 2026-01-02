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
	ctx := r.Context()
	span := spanFromContext(ctx)

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
	_, err = s.directory.LookupHandle(ctx, handle)
	if err == nil {
		s.badRequest(w, fmt.Errorf("handle %q is already taken", in.Handle))
		return
	}
	if !errors.Is(err, identity.ErrHandleNotFound) {
		s.internalErr(w, fmt.Errorf("failed to resolve handle: %w", err))
		return
	}

	// check if the email is already taken
	existingEmail, err := s.db.GetActorByEmail(ctx, *in.Email)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get actor by email: %w", err))
		return
	}
	if existingEmail != nil {
		// @NOTE (jrc): We should send a 200 of some kind here to ensure we're not opening
		// ourselves up to email enumeration attacks. How can we do this in the constraints
		// of the XRPC API?
		s.badRequest(w, fmt.Errorf("invalid create account json"))
		return
	}

	signingKey, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to create signing key: %w", err))
		return
	}

	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to create rotation key: %w", err))
		return
	}

	// create a new did and submit the genesis operation to PLC
	did, plcOp, err := s.plc.CreateDID(ctx, signingKey, rotationKey, "", in.Handle)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to create did: %w", err))
		return
	}
	if err := s.plc.SendOperation(ctx, did, plcOp); err != nil {
		s.internalErr(w, fmt.Errorf("failed to submit plc operation: %w", err))
		return
	}

	pwHash, err := bcrypt.GenerateFromPassword([]byte(*in.Password), bcrypt.DefaultCost)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to hash password: %w", err))
		return
	}

	actor := &types.Actor{
		Did:                   did,
		CreatedAt:             timestamppb.Now(),
		Email:                 *in.Email,
		EmailVerificationCode: fmt.Sprintf("%s-%s", util.RandString(6), util.RandString(6)),
		EmailConfirmed:        false,
		PasswordHash:          pwHash,
		SigningKey:            signingKey.Bytes(),
		Handle:                in.Handle,
		Active:                true,
		RotationKeys:          [][]byte{rotationKey.Bytes()},
	}

	if err := s.db.SaveActor(ctx, actor); err != nil {
		s.internalErr(w, fmt.Errorf("failed to write actor to database: %w", err))
		return
	}

	session, err := s.createSession(ctx, actor)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to create session: %w", err))
		return
	}

	res := atproto.ServerCreateAccount_Output{
		Did:        actor.Did,
		Handle:     actor.Handle,
		AccessJwt:  session.AccessToken,
		RefreshJwt: session.RefreshToken,
	}

	s.jsonOK(w, res)
}

func validateCreateAccountInput(in *atproto.ServerCreateAccount_Input) error {
	switch {
	case in.Email == nil || *in.Email == "":
		return fmt.Errorf("email is required")
	case in.Handle == "":
		return fmt.Errorf("handle is required")
	case in.Password == nil || *in.Password == "":
		return fmt.Errorf("password is required")
	}

	const passLen = 12
	if len(*in.Password) < passLen {
		return fmt.Errorf("password must be at least %d characters", passLen)
	}

	return nil
}
