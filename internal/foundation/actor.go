package foundation

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jcalabro/atlas/internal/types"
	"google.golang.org/protobuf/proto"
)

func (db *DB) SaveActor(ctx context.Context, actor *types.Actor) error {
	_, span := db.tracer.Start(ctx, "SaveActor")
	defer span.End()

	buf, err := proto.Marshal(actor)
	if err != nil {
		return fmt.Errorf("failed to protobuf marshal actor: %w", err)
	}

	actorKey := pack(db.actors, actor.Did)
	didByEmailKey := pack(db.didsByEmail, actor.Email)

	_, err = transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		tx.Set(actorKey, buf)
		tx.Set(didByEmailKey, []byte(actor.Did))
		return nil, nil
	})

	return err
}

func (db *DB) GetActorByEmail(ctx context.Context, email string) (*types.Actor, error) {
	_, span := db.tracer.Start(ctx, "GetActorByEmail")
	defer span.End()

	didByEmailKey := pack(db.didsByEmail, email)

	var actor types.Actor
	ok, err := readProto(db.db, &actor, func(tx fdb.ReadTransaction) ([]byte, error) {
		email, err := tx.Get(didByEmailKey).Get()
		if err != nil {
			return nil, err
		}
		if len(email) == 0 {
			return nil, nil // not found
		}

		actorKey := pack(db.actors, string(email))
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
