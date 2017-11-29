package iavl

import (
	dbm "github.com/tendermint/tmlibs/db"
)

type CachedTree struct {
	// this is the parent to write to
	parent *Tree

	// this is the cache tree we expose
	*Tree
}

// CacheWrap clones this tree with a different
// backing database
func (t *Tree) CacheWrap() *CachedTree {
	// ndb := newNodeDB(t.ndb.nodeCacheSize, t.ndb.db.CacheDB())
	cache := &Tree{
		root: t.root,
		ndb:  t.ndb.Cache(),
	}

	return &CachedTree{
		parent: t,
		Tree:   cache,
	}

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

func (n *nodeDB) Cache() *nodeDB {
	// make a shallow copy, reuse cache, etc.
	cacheDB := *n
	// create a new batch
	cacheDB.batch = &cacheBatch{parent: cacheDB.batch}
	return &cacheDB
}

/////////////////////////////////////////////////
// cacheBatch

// no concurrency please (but fine for abci apps)
type cacheBatch struct {
	parent dbm.Batch
	ops    []operation
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
