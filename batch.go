package iavl

import (
	dbm "github.com/cosmos/cosmos-db"
)

// BatchWithFlusher is a wraper
// around batch that flushes batch's data
// to disk as soon as the configurable limit
// is reached.
type BatchWithFlusher struct {
	db                      dbm.DB    // This is only used to create new batch
	batch                   dbm.Batch // Batched writing buffer.
	batchSizeFlushThreshold int       // The maximum size of the batch in bytes before it gets flushed to disk
}

var _ dbm.Batch = BatchWithFlusher{}

// NewBatchWithFlusher returns new BatchWithFlusher wrapping the passed in batch
func NewBatchWithFlusher(batch dbm.Batch, flushThreshold int) BatchWithFlusher {
	return BatchWithFlusher{
		batch:                   batch,
		batchSizeFlushThreshold: flushThreshold,
	}
}

// Set sets value at the given key to the db.
// The set grows the underlying batch and if batchSizeFlushThreshold is reached,
// the batch is flushed to disk, cleared, and a new one is created with buffer
// pre-allocated to threshold.
func (b BatchWithFlusher) Set(key []byte, value []byte) error {
	size, err := b.batch.GetByteSize()
	if err != nil {
		return err
	}
	if size >= b.batchSizeFlushThreshold {
		b.batch.Write()
		b.batch = b.db.NewBatch()
	}
	err = b.batch.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

// Delete delete value at the given key to the db.
// The Deletion grows the underlying batch and if batchSizeFlushThreshold is reached,
// the batch is flushed to disk, cleared, and a new one is created with buffer
// pre-allocated to threshold.
func (b BatchWithFlusher) Delete(key []byte) error {
	size, err := b.batch.GetByteSize()
	if err != nil {
		return err
	}
	if size >= b.batchSizeFlushThreshold {
		b.batch.Write()
		b.batch = b.db.NewBatch()
	}
	err = b.batch.Delete(key)
	if err != nil {
		return err
	}
	return nil
}
