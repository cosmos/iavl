package iavl

import (
	dbm "github.com/tendermint/tmlibs/db"
)

// CacheWrap clones this tree with a different
// backing database
func (t *Tree) CacheWrap() *CachedTree {
	ndb := t.ndb.clone()
	ndb.batch = newCacheBatch(ndb.batch)

	cache := &Tree{
		root: t.root,
		ndb:  ndb,
	}

	return &CachedTree{
		parent: t,
		Tree:   cache,
	}
}

// Cache clones this tree with a different
// backing database and same type
//
// Nicer to use but more magic
func (t *Tree) Cache() *Tree {
	ndb := t.ndb.clone()
	cache := &Tree{
		root: t.root,
		ndb:  ndb,
	}

	// change batch on the cache...
	ndb.batch = newMagicBatch(ndb.batch, t, cache)

	return cache
}

type CachedTree struct {
	// this is the parent to write to
	parent *Tree

	// this is the cache tree we expose
	*Tree
}

// Write writes pending updates to the parent database
// and clears the cache - meant for cachedbs...
func (c *CachedTree) Write() error {
	// TODO: some sort of checks and balances...
	c.parent.root = c.root
	// this will write our cached batch to the parent batch
	c.Tree.ndb.Commit()
	return nil
}

/////////////////////////////////////////////////
// cacheBatch

// no concurrency please (but fine for abci apps)
type cacheBatch struct {
	parent dbm.Batch
	ops    []operation
}

func newCacheBatch(parent dbm.Batch) *cacheBatch {
	return &cacheBatch{parent: parent}
}

var _ dbm.Batch = (*cacheBatch)(nil)

type opType int

const (
	opTypeSet    opType = 1
	opTypeDelete opType = 2
)

type operation struct {
	opType
	key   []byte
	value []byte
}

func (cb *cacheBatch) Set(key, value []byte) {
	cb.ops = append(cb.ops, operation{opTypeSet, key, value})
}

func (cb *cacheBatch) Delete(key []byte) {
	cb.ops = append(cb.ops, operation{opTypeDelete, key, nil})
}

func (cb *cacheBatch) Write() {
	for _, op := range cb.ops {
		switch op.opType {
		case opTypeSet:
			cb.parent.Set(op.key, op.value)
		case opTypeDelete:
			cb.parent.Delete(op.key)
		}
	}
}

///////////////////////////////////////////////////
// magicBatch also updates trees on Write

type magicBatch struct {
	*cacheBatch
	parent *Tree
	self   *Tree
}

var _ dbm.Batch = (*magicBatch)(nil)

func newMagicBatch(batch dbm.Batch, parent, self *Tree) *magicBatch {
	return &magicBatch{
		cacheBatch: newCacheBatch(batch),
		parent:     parent,
		self:       self,
	}
}

// Commits the batch to the parent batch
// Also copies the iavl tree from cache to parent
func (mb *magicBatch) Write() {
	mb.cacheBatch.Write()
	mb.parent.root = mb.self.root
}
