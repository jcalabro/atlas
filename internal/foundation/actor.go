package foundation

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/gogo/protobuf/proto"
	"github.com/jcalabro/atlas/internal/types"
)

func (db *DB) SaveActor(ctx context.Context, actor *types.Actor) error {
	_, span := db.tracer.Start(ctx, "SaveActor")
	defer span.End()

	buf, err := proto.Marshal(actor)
	if err != nil {
		return fmt.Errorf("failed to protobuf marshal actor: %w", err)
	}

	key := db.actors.Pack(tuple.Tuple{actor.Did})

	_, err = transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		tx.Set(key, buf)
		return nil, nil
	})

	return err
}
