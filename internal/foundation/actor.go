package foundation

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jcalabro/atlas/internal/types"
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

func (db *DB) SaveActor(ctx context.Context, actor *types.Actor) error {
	_, span := db.tracer.Start(ctx, "SaveActor")
	defer span.End()

	if err := ValidateActor(actor); err != nil {
		return fmt.Errorf("invalid actor: %w", err)
	}

	buf, err := proto.Marshal(actor)
	if err != nil {
		return fmt.Errorf("failed to protobuf marshal actor: %w", err)
	}

	actorKey := pack(db.actors.actors, actor.Did)
	didByHandleKey := pack(db.actors.didsByHandle, actor.Handle)
	didByEmailKey := pack(db.actors.didsByEmail, actor.PdsHost, actor.Email)
	didByHostKey := pack(db.actors.didsByHost, actor.PdsHost, actor.Did)

	_, err = transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		tx.Set(actorKey, buf)
		tx.Set(didByHandleKey, []byte(actor.Did))
		tx.Set(didByEmailKey, []byte(actor.Did))
		tx.Set(didByHostKey, []byte{})
		return nil, nil
	})

	return err
}

func (db *DB) GetActorByDID(ctx context.Context, did string) (*types.Actor, error) {
	_, span := db.tracer.Start(ctx, "GetActorByDID")
	defer span.End()

	actorKey := pack(db.actors.actors, did)

	var actor types.Actor
	ok, err := readProto(db.db, &actor, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(actorKey).Get()
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	return &actor, nil
}

func (db *DB) GetActorByEmail(ctx context.Context, pdsHost, email string) (*types.Actor, error) {
	_, span := db.tracer.Start(ctx, "GetActorByEmail")
	defer span.End()

	didByEmailKey := pack(db.actors.didsByEmail, pdsHost, email)

	var actor types.Actor
	ok, err := readProto(db.db, &actor, func(tx fdb.ReadTransaction) ([]byte, error) {
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
	if !ok {
		return nil, nil
	}

	return &actor, nil
}

func (db *DB) GetActorByHandle(ctx context.Context, handle string) (*types.Actor, error) {
	_, span := db.tracer.Start(ctx, "GetActorByHandle")
	defer span.End()

	didByHandleKey := pack(db.actors.didsByHandle, handle)

	var actor types.Actor
	ok, err := readProto(db.db, &actor, func(tx fdb.ReadTransaction) ([]byte, error) {
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
	if !ok {
		return nil, nil
	}

	return &actor, nil
}

func (db *DB) ListActors(ctx context.Context, pdsHost, cursor string, limit int64) ([]*types.Actor, string, error) {
	_, span := db.tracer.Start(ctx, "ListActors")
	defer span.End()

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
		return nil, "", fmt.Errorf("failed to list actors: %w", err)
	}

	actors := make([]*types.Actor, 0, len(bufs))
	for _, buf := range bufs {
		var actor types.Actor
		if err := proto.Unmarshal(buf, &actor); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal actor: %w", err)
		}
		actors = append(actors, &actor)
	}

	// determine the next cursor for pagination
	var nextCursor string
	if int64(len(actors)) > limit {
		nextCursor = actors[limit-1].Did
		actors = actors[:limit]
	}

	return actors, nextCursor, nil
}
