package foundation

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type Config struct {
	ClusterFile string
	APIVersion  int
}

func Open(cfg Config) (fdb.Database, error) {
	if err := fdb.APIVersion(cfg.APIVersion); err != nil {
		return fdb.Database{}, fmt.Errorf("failed to set fdb client api version: %w", err)
	}

	db, err := fdb.OpenDatabase(cfg.ClusterFile)
	if err != nil {
		return fdb.Database{}, fmt.Errorf("failed to initialize fdb client from cluster file %q: %w", cfg.ClusterFile, err)
	}

	const maxTXMillis = 5000
	if err := db.Options().SetTransactionTimeout(maxTXMillis); err != nil {
		return fdb.Database{}, fmt.Errorf("failed to set fdb transaction timeout: %w", err)
	}

	if err := db.Options().SetTransactionRetryLimit(100); err != nil {
		return fdb.Database{}, fmt.Errorf("failed to set fdb transaction retry limit: %w", err)
	}

	_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key("PING")).Get()
	})
	if err != nil {
		return fdb.Database{}, fmt.Errorf("failed to ping foundationdb: %w", err)
	}

	return db, nil
}
