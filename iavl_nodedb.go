package iavl

import (
	"bytes"
	"container/list"
	"sync"

	cmn "github.com/tendermint/tmlibs/common"
	dbm "github.com/tendermint/tmlibs/db"
)

type nodeDB struct {
	mtx         sync.Mutex
	cache       map[string]*list.Element
	cacheSize   int
	cacheQueue  *list.List
	db          dbm.DB
	batch       dbm.Batch
	orphans     map[string]struct{}
	orphansPrev map[string]struct{}
}

func newNodeDB(cacheSize int, db dbm.DB) *nodeDB {
	ndb := &nodeDB{
		cache:       make(map[string]*list.Element),
		cacheSize:   cacheSize,
		cacheQueue:  list.New(),
		db:          db,
		batch:       db.NewBatch(),
		orphans:     make(map[string]struct{}),
		orphansPrev: make(map[string]struct{}),
	}
	return ndb
}

func (ndb *nodeDB) GetNode(hash []byte) *IAVLNode {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	// Check the cache.
	elem, ok := ndb.cache[string(hash)]
	if ok {
		// Already exists. Move to back of cacheQueue.
		ndb.cacheQueue.MoveToBack(elem)
		return elem.Value.(*IAVLNode)
	} else {
		// Doesn't exist, load.
		buf := ndb.db.Get(hash)
		if len(buf) == 0 {
			// ndb.db.Print()
			cmn.PanicSanity(cmn.Fmt("Value missing for key %X", hash))
		}
		node, err := MakeIAVLNode(buf)
		if err != nil {
			cmn.PanicCrisis(cmn.Fmt("Error reading IAVLNode. bytes: %X  error: %v", buf, err))
		}
		node.hash = hash
		node.persisted = true
		ndb.cacheNode(node)
		return node
	}
}

func (ndb *nodeDB) SaveNode(node *IAVLNode) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	if node.hash == nil {
		cmn.PanicSanity("Expected to find node.hash, but none found.")
	}
	if node.persisted {
		cmn.PanicSanity("Shouldn't be calling save on an already persisted node.")
	}
	/*if _, ok := ndb.cache[string(node.hash)]; ok {
		panic("Shouldn't be calling save on an already cached node.")
	}*/
	// Save node bytes to db
	buf := bytes.NewBuffer(nil)
	_, err := node.writePersistBytes(buf)
	if err != nil {
		cmn.PanicCrisis(err)
	}
	ndb.batch.Set(node.hash, buf.Bytes())
	node.persisted = true
	ndb.cacheNode(node)
	// Re-creating the orphan,
	// Do not garbage collect.
	delete(ndb.orphans, string(node.hash))
	delete(ndb.orphansPrev, string(node.hash))
}

func (ndb *nodeDB) RemoveNode(t *IAVLTree, node *IAVLNode) {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	if node.hash == nil {
		cmn.PanicSanity("Expected to find node.hash, but none found.")
	}
	if !node.persisted {
		cmn.PanicSanity("Shouldn't be calling remove on a non-persisted node.")
	}
	elem, ok := ndb.cache[string(node.hash)]
	if ok {
		ndb.cacheQueue.Remove(elem)
		delete(ndb.cache, string(node.hash))
	}
	ndb.orphans[string(node.hash)] = struct{}{}
}

func (ndb *nodeDB) cacheNode(node *IAVLNode) {
	// Create entry in cache and append to cacheQueue.
	elem := ndb.cacheQueue.PushBack(node)
	ndb.cache[string(node.hash)] = elem
	// Maybe expire an item.
	if ndb.cacheQueue.Len() > ndb.cacheSize {
		hash := ndb.cacheQueue.Remove(ndb.cacheQueue.Front()).(*IAVLNode).hash
		delete(ndb.cache, string(hash))
	}
}

func (ndb *nodeDB) Commit() {
	ndb.mtx.Lock()
	defer ndb.mtx.Unlock()
	// Delete orphans from previous block
	for orphanHashStr, _ := range ndb.orphansPrev {
		ndb.batch.Delete([]byte(orphanHashStr))
	}
	// Write saves & orphan deletes
	ndb.batch.Write()
	ndb.db.SetSync(nil, nil)
	ndb.batch = ndb.db.NewBatch()
	// Shift orphans
	ndb.orphansPrev = ndb.orphans
	ndb.orphans = make(map[string]struct{})
}
