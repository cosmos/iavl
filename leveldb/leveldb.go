//go:build leveldb
// +build leveldb

// Package leveldb provides a leveldb implementation of the database interface. It should not be in this module but is
// included here for now for benchmarks and testing.
package leveldb

import (
	dbm "github.com/cosmos/cosmos-db"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func New(name, dir string, options *opt.Options) (*dbm.GoLevelDB, error) {
	return dbm.NewGoLevelDBWithOpts(name, dir, options)
}
