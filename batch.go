package iavl

import (
	dbm "github.com/cosmos/cosmos-db"
)

// BatchWithFlusher is a wrapper
// around batch that flushes batch's data to disk
// as soon as the configurable limit is reached.
type BatchWithFlusher struct {
	db             dbm.DB    // This is only used to create new batch
	batch          dbm.Batch // Batched writing buffer.
	flushThreshold int       // The maximum size of the batch in bytes before it gets flushed to disk
}

var _ dbm.Batch = &BatchWithFlusher{}

// Ethereum has found that commit of 100KB is optimal, ref ethereum/go-ethereum#15115
var DefaultFlushThreshold = 100000

// NewBatchWithFlusher returns new BatchWithFlusher wrapping the passed in batch
func NewBatchWithFlusher(db dbm.DB, flushThreshold int) *BatchWithFlusher {
	if flushThreshold <= 0 {
		panic("flushThreshold can't be zero or negative")
	}
	return &BatchWithFlusher{
		db:             db,
		batch:          db.NewBatchWithSize(flushThreshold),
		flushThreshold: flushThreshold,
	}
}

// estimateSizeAfterSetting estimates the batch's size after setting a key / value
func (b *BatchWithFlusher) estimateSizeAfterSetting(key []byte, value []byte) (int, error) {
	currentSize, err := b.batch.GetByteSize()
	if err != nil {
		return 0, err
	}
	// for some batch implementation, when adding a key / value,
	// the batch size could gain more than the total size of key and value,
	// https://github.com/syndtr/goleveldb/blob/64ee5596c38af10edb6d93e1327b3ed1739747c7/leveldb/batch.go#L98

	// we add 100 here just to over-account for that overhead
	// since estimateSizeAfterSetting is only used to check if we exceed the threshold when setting a key / value
	// this means we only over-account for the last key / value
	return currentSize + len(key) + len(value) + 100, nil
}

// Set sets value at the given key to the db.
// If the set causes the underlying batch size to exceed batchSizeFlushThreshold,
// the batch is flushed to disk, cleared, and a new one is created with buffer pre-allocated to threshold.
// The addition entry is then added to the batch.
func (b *BatchWithFlusher) Set(key []byte, value []byte) error {
	batchSizeAfter, err := b.estimateSizeAfterSetting(key, value)
	if err != nil {
		return err
	}
	if batchSizeAfter > b.flushThreshold {
		err = b.batch.Write()
		if err != nil {
			return err
		}
		err = b.batch.Close()
		if err != nil {
			return err
		}
		b.batch = b.db.NewBatchWithSize(b.flushThreshold)
	}
	err = b.batch.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

// Delete delete value at the given key to the db.
// If the deletion causes the underlying batch size to exceed batchSizeFlushThreshold,
// the batch is flushed to disk, cleared, and a new one is created with buffer pre-allocated to threshold.
// The deletion entry is then added to the batch.
func (b *BatchWithFlusher) Delete(key []byte) error {
	batchSizeAfter, err := b.estimateSizeAfterSetting(key, []byte{})
	if err != nil {
		return err
	}
	if batchSizeAfter > b.flushThreshold {
		err = b.batch.Write()
		if err != nil {
			return err
		}
		err = b.batch.Close()
		if err != nil {
			return err
		}
		b.batch = b.db.NewBatchWithSize(b.flushThreshold)
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
