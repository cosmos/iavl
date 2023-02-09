package iavl

import (
	dbm "github.com/cosmos/cosmos-db"
)

// BatchWithFlusher is a wrapper
// around batch that flushes batch's data to disk
// as soon as the configurable limit is reached.
type BatchWithFlusher struct {
	db                      dbm.DB    // This is only used to create new batch
	batch                   dbm.Batch // Batched writing buffer.
	batchSizeFlushThreshold int       // The maximum size of the batch in bytes before it gets flushed to disk
}

var _ dbm.Batch = &BatchWithFlusher{}

// this value is used to estimate the additional size when adding an entry to a batch
// additional size = key length + value length + possibleEntryOverHead
var possibleEntryOverHead = 100

// Ethereum has found that commit of 100KB is optimal, ref ethereum/go-ethereum#15115
var defaultFlushThreshold = 100000

// NewBatchWithFlusher returns new BatchWithFlusher wrapping the passed in batch
func NewBatchWithFlusher(db dbm.DB, flushThreshold int) *BatchWithFlusher {
	return &BatchWithFlusher{
		db:                      db,
		batch:                   db.NewBatchWithSize(flushThreshold),
		batchSizeFlushThreshold: flushThreshold,
	}
}

// Set sets value at the given key to the db.
// If the set causes the underlying batch size to exceed batchSizeFlushThreshold would be reached,
// the batch is flushed to disk, cleared, and a new one is created with buffer pre-allocated to threshold.
// The addition entry is then added to the batch.
func (b *BatchWithFlusher) Set(key []byte, value []byte) error {
	size, err := b.batch.GetByteSize()
	if err != nil {
		return err
	}
	if size+len(key)+len(value)+possibleEntryOverHead >= b.batchSizeFlushThreshold {
		err = b.batch.Write()
		if err != nil {
			return err
		}
		err = b.batch.Close()
		if err != nil {
			return err
		}
		b.batch = b.db.NewBatchWithSize(b.batchSizeFlushThreshold)
	}
	err = b.batch.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

// Delete delete value at the given key to the db.
// If the deletion causes the underlying batch size to exceed batchSizeFlushThreshold would be reached,
// the batch is flushed to disk, cleared, and a new one is created with buffer pre-allocated to threshold.
// The deletion entry is then added to the batch.
func (b *BatchWithFlusher) Delete(key []byte) error {
	size, err := b.batch.GetByteSize()
	if err != nil {
		return err
	}
	if size+len(key)+possibleEntryOverHead >= b.batchSizeFlushThreshold {
		err = b.batch.Write()
		if err != nil {
			return err
		}
		b.batch = b.db.NewBatchWithSize(b.batchSizeFlushThreshold)
	}
	err = b.batch.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (b *BatchWithFlusher) Write() error {
	return b.batch.Write()
}

func (b *BatchWithFlusher) WriteSync() error {
	return b.batch.WriteSync()
}

func (b *BatchWithFlusher) Close() error {
	return b.batch.Close()
}

func (b *BatchWithFlusher) GetByteSize() (int, error) {
	return b.batch.GetByteSize()
}
