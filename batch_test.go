package iavl

import (
	"encoding/binary"
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

var bytesArrayOfSize10KB = [10000]byte{}

func BenchmarkBatchWithFlusher(b *testing.B) {
	testedBackends := []dbm.BackendType{
		dbm.GoLevelDBBackend,
	}

	for _, backend := range testedBackends {
		benchmarkBatchWithFlusher(b, backend)
	}
}

func benchmarkBatchWithFlusher(b *testing.B, backend dbm.BackendType) {
	name := fmt.Sprintf("test_%x", randstr(12))
	dir := b.TempDir()
	db, err := dbm.NewDB(name, backend, dir)
	require.NoError(b, err)
	defer cleanupDBDir(dir, name)

	batchWithFlusher := NewBatchWithFlusher(db, defaultFlushThreshold)

	// we'll try to to commit 10MBs of data into the db
	for n := uint16(0); n < 1000; n++ {
		// each key / value is 10 KBs
		key := make([]byte, 4)
		binary.BigEndian.PutUint16(key, n)
		batchWithFlusher.Set(key, bytesArrayOfSize10KB[:])
	}
	batchWithFlusher.Write()
}
