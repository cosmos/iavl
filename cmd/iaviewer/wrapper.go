package main

import (
	corestore "cosmossdk.io/core/store"
	dbm "github.com/cosmos/cosmos-db"
)

// Wrapper wraps a dbm.DB to implement DB.
type Wrapper struct {
	dbm.DB
}

// newWrapper returns a new Wrapper.
func newWrapper(db dbm.DB) *Wrapper {
	return &Wrapper{DB: db}
}

// Iterator implements DB.
func (db *Wrapper) Iterator(start, end []byte) (corestore.Iterator, error) {
	return db.DB.Iterator(start, end)
}

// ReverseIterator implements DB.
func (db *Wrapper) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	return db.DB.ReverseIterator(start, end)
}

// NewBatch implements DB.
func (db *Wrapper) NewBatch() corestore.Batch {
	return db.DB.NewBatch()
}

// NewBatchWithSize implements DB.
func (db *Wrapper) NewBatchWithSize(size int) corestore.Batch {
	return db.DB.NewBatchWithSize(size)
}
