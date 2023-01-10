package iavl

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
)

func cleanupDBDir(dir, name string) {
	err := os.RemoveAll(filepath.Join(dir, name) + ".db")
	if err != nil {
		panic(err)
	}
}

func BenchmarkBatchWithFlusher(b *testing.B) {

}

func benchmarkBatchWithFlusher(b *testing.B, backend dbm.BackendType) {
	name := fmt.Sprintf("test_%x", randstr(12))
	dir := os.TempDir()
	db, err := dbm.NewDB(name, backend, dir)
	require.NoError(b, err)
	defer cleanupDBDir(dir, name)

	batchWithFlusher := NewBatchWithFlusher(db, defaultFlushThreshold)

	// we'll try to to commit 100MBs of data into the db
	for n := 0; n < 1000; n++ {
		// each
		batchWithFlusher.Set(byte(n))
	}
}
