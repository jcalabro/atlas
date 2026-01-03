package foundation

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"go.opentelemetry.io/otel/attribute"
)

// NextTID generates the next TID for a repo, ensuring strict monotonicity
// across all PDS processes by using FDB atomic operations.
//
// The TID is constructed from the current timestamp (microseconds) with the
// low 10 bits reserved for clock ID (set to 0). If the timestamp-based TID
// would not be greater than the last generated TID for this repo, we
// increment from the last value instead to maintain monotonicity.
func (db *DB) NextTID(ctx context.Context, did string) (syntax.TID, error) {
	_, span := db.tracer.Start(ctx, "NextTID")
	defer span.End()

	span.SetAttributes(attribute.String("did", did))

	key := pack(db.actors.tidsByDID, did)

	newTID, err := transaction(db.db, func(tx fdb.Transaction) (uint64, error) {
		// read current last TID for this repo
		val, err := tx.Get(key).Get()
		if err != nil {
			return 0, err
		}

		var lastTID uint64
		if len(val) == 8 {
			lastTID = binary.BigEndian.Uint64(val)
		}

		// generate candidate TID from current time (clock ID = 0)
		nowMicros := time.Now().UTC().UnixMicro()
		candidate := uint64(nowMicros&0x1F_FFFF_FFFF_FFFF) << 10

		// ensure monotonicity: new TID must be greater than last
		var tid uint64
		if candidate > lastTID {
			tid = candidate
		} else {
			tid = lastTID + 1
		}

		// write back
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, tid)
		tx.Set(key, buf)

		return tid, nil
	})
	if err != nil {
		return "", err
	}

	span.SetAttributes(attribute.Int64("tid", int64(newTID)))

	return syntax.NewTIDFromInteger(newTID), nil
}
