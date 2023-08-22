package cache

import "sync"

// WriteNode is a node that can be written to the cache.
type WriteNode interface {
	Node

	// GetVersion returns the version of the node.
	GetVersion() int64
}

// NodeCache is a cache for writing nodes asynchronously.
type NodeCache interface {
	Cache

	// SetVersion sets the version of the cache that allows it to delete nodes.
	SetVersion(version int64)
}

// nodeCache is a wrapper around a Cache.
// It is used to maintain nodes up to a certain version for asynchonous writes.
type nodeCache struct {
	c *lruCache

	version int64 // can delete nodes from cache up to this version
	mtx     sync.RWMutex
}

// NewNodeCache returns a new nodeCache.
func NewNodeCache(maxElementCount int) NodeCache {
	return &nodeCache{
		c: New(maxElementCount).(*lruCache),
	}
}

// Add adds a node to the cache.
func (nc *nodeCache) Add(node Node) Node {
	nc.mtx.Lock()
	defer nc.mtx.Unlock()

	if e, exists := nc.c.dict[string(node.GetKey())]; exists {
		nc.c.ll.MoveToFront(e)
		old := e.Value
		e.Value = node
		return old.(Node)
	}

	elem := nc.c.ll.PushFront(node)
	nc.c.dict[string(node.GetKey())] = elem

	for nc.c.Len() > nc.c.maxElementCount {
		oldest := nc.c.ll.Back()
		if oldest.Value.(WriteNode).GetVersion() > nc.version {
			break
		}
		nc.c.remove(oldest)
	}

	return nil
}

// Get gets a node from the cache.
func (nc *nodeCache) Get(key []byte) Node {
	nc.mtx.RLock()
	defer nc.mtx.RUnlock()

	return nc.c.Get(key)
}

// Has returns true if the cache has the key.
func (nc *nodeCache) Has(key []byte) bool {
	nc.mtx.RLock()
	defer nc.mtx.RUnlock()

	return nc.c.Has(key)
}

// Len returns the number of nodes in the cache.
func (nc *nodeCache) Len() int {
	nc.mtx.RLock()
	defer nc.mtx.RUnlock()

	return nc.c.Len()
}

// Remove removes a node from the cache.
func (nc *nodeCache) Remove(key []byte) Node {
	nc.mtx.Lock()
	defer nc.mtx.Unlock()

	return nc.c.Remove(key)
}

// SetVersion sets the version of the cache.
func (nc *nodeCache) SetVersion(version int64) {
	nc.mtx.Lock()
	defer nc.mtx.Unlock()

	nc.version = version
}
