package foundation

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type Config struct {
	ClusterFile           string
	APIVersion            int
	TransactionTimeout    int64
	TransactionRetryLimit int64
}

func Open(cfg Config) (fdb.Database, error) {
	if err := fdb.APIVersion(cfg.APIVersion); err != nil {
		return fdb.Database{}, fmt.Errorf("failed to set fdb client api version: %w", err)
	}

	db, err := fdb.OpenDatabase(cfg.ClusterFile)
	if err != nil {
		return fdb.Database{}, fmt.Errorf("failed to initialize fdb client from cluster file %q: %w", cfg.ClusterFile, err)
	}

	_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key("PING")).Get()
	})
	if err != nil {
		return fdb.Database{}, fmt.Errorf("failed to ping foundationdb: %w", err)
	}

	if err := db.Options().SetTransactionTimeout(cfg.TransactionTimeout); err != nil {
		return fdb.Database{}, fmt.Errorf("failed to set fdb transaction timeout: %w", err)
	}
	if err := db.Options().SetTransactionRetryLimit(cfg.TransactionRetryLimit); err != nil {
		return fdb.Database{}, fmt.Errorf("failed to set fdb transaction retry limit: %w", err)
	}

	return db, nil
}
