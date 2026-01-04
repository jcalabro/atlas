package foundation

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jcalabro/atlas/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
)

func ValidateActor(a *types.Actor) error {
	switch {
	case a == nil:
		return fmt.Errorf("actor is nil")
	case a.CreatedAt == nil:
		return fmt.Errorf("created timestamp is required")
	case a.Did == "":
		return fmt.Errorf("did is required")
	case a.Email == "":
		return fmt.Errorf("email is required")
	case a.Handle == "":
		return fmt.Errorf("handle is required")
	case a.PdsHost == "":
		return fmt.Errorf("pds_host is required")
	case len(a.PasswordHash) == 0:
		return fmt.Errorf("password hash is required")
	case len(a.SigningKey) == 0:
		return fmt.Errorf("signing key is required")
	case !atLeastOneByteSlice(a.RotationKeys):
		return fmt.Errorf("at least one rotation key is required")
	}

	return nil
}

func (db *DB) SaveActor(ctx context.Context, actor *types.Actor) (err error) {
	_, span, done := db.observe(ctx, "SaveActor")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", actor.Did),
		attribute.String("handle", actor.Handle),
		attribute.Bool("email_confirmed", actor.EmailConfirmed),
		attribute.Bool("active", actor.Active),
		attribute.String("head", actor.Head),
		attribute.String("rev", actor.Rev),
		attribute.String("pds_host", actor.PdsHost),
	)

	if err = ValidateActor(actor); err != nil {
		err = fmt.Errorf("invalid actor: %w", err)
		return
	}

	_, err = transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		return nil, db.saveActorTx(tx, actor)
	})

	return
}

// Saves an actor using an existing transaction
func (db *DB) saveActorTx(tx fdb.Transaction, actor *types.Actor) error {
	buf, err := proto.Marshal(actor)
	if err != nil {
		return fmt.Errorf("failed to protobuf marshal actor: %w", err)
	}

	actorKey := pack(db.actors.actors, actor.Did)
	didByHandleKey := pack(db.actors.didsByHandle, actor.Handle)
	didByEmailKey := pack(db.actors.didsByEmail, actor.PdsHost, actor.Email)
	didByHostKey := pack(db.actors.didsByHost, actor.PdsHost, actor.Did)

	tx.Set(actorKey, buf)
	tx.Set(didByHandleKey, []byte(actor.Did))
	tx.Set(didByEmailKey, []byte(actor.Did))
	tx.Set(didByHostKey, []byte{})

	return nil
}

// Returns the actor with the given DID, with reads executed using the given transaction
func (db *DB) getActorByDIDTx(tx fdb.ReadTransaction, did string) (*types.Actor, error) {
	actorKey := pack(db.actors.actors, did)
	buf, err := tx.Get(actorKey).Get()
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, nil
	}

	var actor types.Actor
	if err := proto.Unmarshal(buf, &actor); err != nil {
		return nil, err
	}

	return &actor, nil
}

func (db *DB) GetActorByDID(ctx context.Context, did string) (actor *types.Actor, err error) {
	_, span, done := db.observe(ctx, "GetActorByDID")
	defer func() { done(err) }()

	span.SetAttributes(attribute.String("did", actor.Did))

	actorKey := pack(db.actors.actors, did)

	var a types.Actor
	err = readProto(db.db, &a, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(actorKey).Get()
	})
	if err != nil {
		return nil, err
	}

	actor = &a
	return
}

func (db *DB) GetActorByEmail(ctx context.Context, pdsHost, email string) (actor *types.Actor, err error) {
	_, span, done := db.observe(ctx, "GetActorByEmail")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("pds_host", pdsHost),
		attribute.String("email", email),
	)

	didByEmailKey := pack(db.actors.didsByEmail, pdsHost, email)

	var a types.Actor
	err = readProto(db.db, &a, func(tx fdb.ReadTransaction) ([]byte, error) {
		did, err := tx.Get(didByEmailKey).Get()
		if err != nil {
			return nil, err
		}
		if len(did) == 0 {
			return nil, nil // not found
		}

		actorKey := pack(db.actors.actors, string(did))
		return tx.Get(actorKey).Get()
	})
	if err != nil {
		return nil, err
	}

	actor = &a
	return
}

func (db *DB) GetActorByHandle(ctx context.Context, handle string) (actor *types.Actor, err error) {
	_, span, done := db.observe(ctx, "GetActorByHandle")
	defer func() { done(err) }()

	span.SetAttributes(attribute.String("handle", handle))

	didByHandleKey := pack(db.actors.didsByHandle, handle)

	var a types.Actor
	err = readProto(db.db, &a, func(tx fdb.ReadTransaction) ([]byte, error) {
		did, err := tx.Get(didByHandleKey).Get()
		if err != nil {
			return nil, err
		}
		if len(did) == 0 {
			return nil, nil // not found
		}

		actorKey := pack(db.actors.actors, string(did))
		return tx.Get(actorKey).Get()
	})
	if err != nil {
		return nil, err
	}

	actor = &a
	return
}

func (db *DB) ListActors(
	ctx context.Context,
	pdsHost,
	cursor string,
	limit int64,
) (actors []*types.Actor, nextCursor string, err error) {
	_, span, done := db.observe(ctx, "ListActors")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("pds_host", pdsHost),
		attribute.String("cursor", cursor),
		attribute.Int64("limit", limit),
	)

	bufs, err := readTransaction(db.db, func(tx fdb.ReadTransaction) ([][]byte, error) {
		// create range for all keys with this pds_host prefix
		rangeBegin := pack(db.actors.didsByHost, pdsHost)
		rangeEnd := pack(db.actors.didsByHost, pdsHost+"\xff")

		var begin fdb.KeyConvertible
		if cursor == "" {
			// start from the very beginning
			begin = rangeBegin
		} else {
			// start from the key after the cursor (exclusive)
			cursorKey := pack(db.actors.didsByHost, pdsHost, cursor)
			begin = fdb.Key(append(cursorKey, 0x00))
		}

		kr := fdb.KeyRange{Begin: begin, End: rangeEnd}
		opts := fdb.RangeOptions{Limit: int(limit + 1)}

		var futures []fdb.FutureByteSlice
		iter := tx.GetRange(kr, opts).Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			// extract the DID from the key tuple (pds_host, did)
			tup, err := db.actors.didsByHost.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}
			if len(tup) < 2 {
				continue
			}
			did, ok := tup[1].(string)
			if !ok {
				continue
			}

			actorKey := pack(db.actors.actors, did)
			futures = append(futures, tx.Get(actorKey))
		}

		// resolve all futures and collect raw bytes
		out := make([][]byte, 0, len(futures))
		for _, fut := range futures {
			buf, err := fut.Get()
			if err != nil {
				return nil, err
			}
			if len(buf) > 0 {
				out = append(out, buf)
			}
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	// unmarshal result buffers
	results := make([]*types.Actor, 0, len(bufs))
	for _, buf := range bufs {
		var actor types.Actor
		if err = proto.Unmarshal(buf, &actor); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal actor: %w", err)
		}
		results = append(results, &actor)
	}

	// determine the next cursor for pagination
	if int64(len(results)) > limit {
		nextCursor = results[limit-1].Did
		results = results[:limit]
	}

	actors = results
	return
}
