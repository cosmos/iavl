package iavl

import (
	"fmt"
	"math/rand"
	"testing"

	dbm "github.com/cosmos/iavl/db"
	"github.com/stretchr/testify/require"
)

type MockDBBatch struct {
	dbm.Batch

	// simulate low level system error
	err error
}

func (b *MockDBBatch) Write() error {
	if b.err != nil {
		return b.err
	}
	return b.Batch.Write()
}

func (b *MockDBBatch) WriteSync() error {
	if b.err != nil {
		return b.err
	}
	return b.Batch.WriteSync()
}

type MockDB struct {
	dbm.DB

	batchIndex int
	// simulate low level system error at i-th batch write
	errors map[int]error
}

func (db *MockDB) NewBatch() dbm.Batch {
	err, _ := db.errors[db.batchIndex]
	batch := &MockDBBatch{Batch: db.DB.NewBatch(), err: err}
	db.batchIndex++
	return batch
}

func (db *MockDB) NewBatchWithSize(size int) dbm.Batch {
	return db.NewBatch()
}

func TestBatchFlusher(t *testing.T) {
	db := &MockDB{DB: dbm.NewMemDB()}

	{
		tree := NewMutableTree(db, 10000, true, NewNopLogger())
		v, err := tree.Load()
		require.NoError(t, err)
		require.Equal(t, int64(0), v)

		// the batch size exceeds the threshold, and persist into db in two batches
		for i := 0; i < 1000; i++ {
			// random key value pairs
			key := []byte(fmt.Sprintf("key-%064d", rand.Intn(100000000)))
			value := []byte(fmt.Sprintf("value-%064d", rand.Intn(100000000)))
			_, err := tree.Set(key, value)
			require.NoError(t, err)
		}

		// the first batch write will success,
		// the second batch write will fail
		db.errors = map[int]error{
			1: fmt.Errorf("filesystem failure"),
		}
		_, _, err = tree.SaveVersion()
		require.Error(t, err)
	}

	{
		tree := NewMutableTree(db, 10000, true, NewNopLogger())
		v, err := tree.Load()
		require.NoError(t, err)
		require.Equal(t, int64(0), v)
	}
}
