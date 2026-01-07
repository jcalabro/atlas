package db

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jcalabro/atlas/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
)

func (db *DB) SaveBlob(ctx context.Context, blob *types.Blob) (err error) {
	_, span, done := db.observe(ctx, "SaveBlob")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", blob.Did),
		attribute.String("mime_type", blob.MimeType),
		attribute.Int64("size", blob.Size),
	)

	buf, err := proto.Marshal(blob)
	if err != nil {
		return fmt.Errorf("failed to marshal blob: %w", err)
	}

	// key: (did, cid)
	blobKey := pack(db.blobs, blob.Did, blob.Cid)

	_, err = transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		tx.Set(blobKey, buf)
		return nil, nil
	})

	return
}

func (db *DB) GetBlob(ctx context.Context, did string, cid []byte) (blob *types.Blob, err error) {
	_, span, done := db.observe(ctx, "GetBlob")
	defer func() { done(err) }()

	span.SetAttributes(attribute.String("did", did))

	blobKey := pack(db.blobs, did, cid)

	var b types.Blob
	err = readProto(db.db, &b, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(blobKey).Get()
	})
	if err != nil {
		return nil, err
	}

	blob = &b
	return
}

func (db *DB) ListBlobs(
	ctx context.Context,
	did string,
	cursor string,
	limit int,
) (blobs []*types.Blob, nextCursor string, err error) {
	_, span, done := db.observe(ctx, "ListBlobs")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.String("cursor", cursor),
		attribute.Int("limit", limit),
	)

	bufs, err := readTransaction(db.db, func(tx fdb.ReadTransaction) ([][]byte, error) {
		// create range for all blobs with this DID prefix
		rangeBegin := pack(db.blobs, did)
		rangeEnd := pack(db.blobs, did+"\xff")

		var begin fdb.KeyConvertible
		if cursor == "" {
			begin = rangeBegin
		} else {
			// cursor is the CID string, convert to bytes for key
			cursorKey := pack(db.blobs, did, []byte(cursor))
			begin = fdb.Key(append(cursorKey, 0x00))
		}

		kr := fdb.KeyRange{Begin: begin, End: rangeEnd}
		opts := fdb.RangeOptions{Limit: limit + 1}

		iter := tx.GetRange(kr, opts).Iterator()
		out := make([][]byte, 0, limit+1)
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}
			out = append(out, kv.Value)
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	// unmarshal results
	results := make([]*types.Blob, 0, len(bufs))
	for _, buf := range bufs {
		var blob types.Blob
		if err = proto.Unmarshal(buf, &blob); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal blob: %w", err)
		}
		results = append(results, &blob)
	}

	// determine the next cursor
	if len(results) > limit {
		// cursor is the raw CID bytes as string for simplicity
		nextCursor = string(results[limit-1].Cid)
		results = results[:limit]
	}

	blobs = results
	return
}
