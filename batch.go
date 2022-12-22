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

// this value is used to estimate the additional size when adding an entry to a batch
// additional size = key lenght + value lenght + possibleEntryOverHead
var possibleEntryOverHead = 100

// NewBatchWithFlusher returns new BatchWithFlusher wrapping the passed in batch
func NewBatchWithFlusher(batch dbm.Batch, flushThreshold int) BatchWithFlusher {
	return BatchWithFlusher{
		batch:                   batch,
		batchSizeFlushThreshold: flushThreshold,
	}
}

// Set sets value at the given key to the db.
// If the set causes the underlying batch size to exceed batchSizeFlushThreshold would be reached,
// the batch is flushed to disk, cleared, and a new one is created with buffer pre-allocated to threshold.
// The addition entry is then added to the batch.
func (b BatchWithFlusher) Set(key []byte, value []byte) error {
	size, err := b.batch.GetByteSize()
	if err != nil {
		return err
	}
	if size+len(key)+len(value)+possibleEntryOverHead >= b.batchSizeFlushThreshold {
		err = b.batch.Write()
		if err != nil {
			return err
		}
		b.batch = b.db.NewBatch()
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
func (b BatchWithFlusher) Delete(key []byte) error {
	size, err := b.batch.GetByteSize()
	if err != nil {
		return err
	}
	if size+len(key)+possibleEntryOverHead >= b.batchSizeFlushThreshold {
		err = b.batch.Write()
		if err != nil {
			return err
		}
		b.batch = b.db.NewBatch()
	}
	err = b.batch.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (b BatchWithFlusher) Write() error {
	return b.batch.Write()
}

func (b BatchWithFlusher) WriteSync() error {
	return b.batch.WriteSync()
}

func (b BatchWithFlusher) Close() error {
	return b.batch.Close()
}

func (b BatchWithFlusher) GetByteSize() (int, error) {
	return b.batch.GetByteSize()
}
