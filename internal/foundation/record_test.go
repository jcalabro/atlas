package foundation

import (
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSaveAndGetRecord(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testDB(t)

	record := &types.Record{
		Did:        "did:plc:testrecord1",
		Collection: "app.bsky.feed.post",
		Rkey:       "3jui7kd2xs22b",
		Cid:        "bafyreihxrxqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcq",
		Value:      []byte(`{"$type":"app.bsky.feed.post","text":"Hello World","createdAt":"2024-01-01T00:00:00Z"}`),
		CreatedAt:  timestamppb.New(time.Now()),
	}

	// save record
	err := db.SaveRecord(ctx, record)
	require.NoError(t, err)

	// get record
	uri := "at://" + record.Did + "/" + record.Collection + "/" + record.Rkey
	retrieved, err := db.GetRecord(ctx, uri)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, record.Did, retrieved.Did)
	require.Equal(t, record.Collection, retrieved.Collection)
	require.Equal(t, record.Rkey, retrieved.Rkey)
	require.Equal(t, record.Cid, retrieved.Cid)
	require.Equal(t, record.Value, retrieved.Value)
}

func TestGetRecord_NotFound(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testDB(t)

	retrieved, err := db.GetRecord(ctx, "at://did:plc:nonexistent/app.bsky.feed.post/nonexistent")
	require.NoError(t, err)
	require.Nil(t, retrieved)
}

func TestDeleteRecordTx(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testDB(t)

	record := &types.Record{
		Did:        "did:plc:testrecorddelete",
		Collection: "app.bsky.feed.post",
		Rkey:       "3jui7kd2xs22c",
		Cid:        "bafyreihxrxqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcq",
		Value:      []byte(`{"$type":"app.bsky.feed.post","text":"To be deleted"}`),
		CreatedAt:  timestamppb.New(time.Now()),
	}

	// save record
	err := db.SaveRecord(ctx, record)
	require.NoError(t, err)

	// verify it exists
	uri := "at://" + record.Did + "/" + record.Collection + "/" + record.Rkey
	retrieved, err := db.GetRecord(ctx, uri)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	// delete record using DeleteRecordTx within a transaction
	err = db.Transact(func(tx fdb.Transaction) error {
		db.DeleteRecordTx(tx, record.URI())
		return nil
	})
	require.NoError(t, err)

	// verify it's gone
	retrieved, err = db.GetRecord(ctx, uri)
	require.NoError(t, err)
	require.Nil(t, retrieved)
}

func TestValidateRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		record  *types.Record
		wantErr string
	}{
		{
			name:    "nil record",
			record:  nil,
			wantErr: "record is nil",
		},
		{
			name: "missing did",
			record: &types.Record{
				Collection: "app.bsky.feed.post",
				Rkey:       "123",
				Cid:        "bafyrei...",
				Value:      []byte("{}"),
				CreatedAt:  timestamppb.Now(),
			},
			wantErr: "did is required",
		},
		{
			name: "missing collection",
			record: &types.Record{
				Did:       "did:plc:test",
				Rkey:      "123",
				Cid:       "bafyrei...",
				Value:     []byte("{}"),
				CreatedAt: timestamppb.Now(),
			},
			wantErr: "collection is required",
		},
		{
			name: "missing rkey",
			record: &types.Record{
				Did:        "did:plc:test",
				Collection: "app.bsky.feed.post",
				Cid:        "bafyrei...",
				Value:      []byte("{}"),
				CreatedAt:  timestamppb.Now(),
			},
			wantErr: "rkey is required",
		},
		{
			name: "missing cid",
			record: &types.Record{
				Did:        "did:plc:test",
				Collection: "app.bsky.feed.post",
				Rkey:       "123",
				Value:      []byte("{}"),
				CreatedAt:  timestamppb.Now(),
			},
			wantErr: "cid is required",
		},
		{
			name: "missing value",
			record: &types.Record{
				Did:        "did:plc:test",
				Collection: "app.bsky.feed.post",
				Rkey:       "123",
				Cid:        "bafyrei...",
				CreatedAt:  timestamppb.Now(),
			},
			wantErr: "value is required",
		},
		{
			name: "missing created_at",
			record: &types.Record{
				Did:        "did:plc:test",
				Collection: "app.bsky.feed.post",
				Rkey:       "123",
				Cid:        "bafyrei...",
				Value:      []byte("{}"),
			},
			wantErr: "created_at is required",
		},
		{
			name: "valid record",
			record: &types.Record{
				Did:        "did:plc:test",
				Collection: "app.bsky.feed.post",
				Rkey:       "123",
				Cid:        "bafyrei...",
				Value:      []byte("{}"),
				CreatedAt:  timestamppb.Now(),
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateRecord(tt.record)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestSaveRecord_MultipleCollections(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testDB(t)

	did := "did:plc:multicollection"

	// save records in different collections
	collections := []string{
		"app.bsky.feed.post",
		"app.bsky.feed.like",
		"app.bsky.graph.follow",
	}

	for i, collection := range collections {
		record := &types.Record{
			Did:        did,
			Collection: collection,
			Rkey:       "3jui7kd2xs22x",
			Cid:        "bafyreihxrxqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcqzqq5xhcq",
			Value:      []byte(`{"$type":"` + collection + `","index":` + string(rune('0'+i)) + `}`),
			CreatedAt:  timestamppb.New(time.Now()),
		}

		err := db.SaveRecord(ctx, record)
		require.NoError(t, err)
	}

	// verify each record can be retrieved independently
	for _, collection := range collections {
		uri := "at://" + did + "/" + collection + "/" + "3jui7kd2xs22x"
		retrieved, err := db.GetRecord(ctx, uri)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, collection, retrieved.Collection)
	}
}
