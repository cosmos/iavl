package cache

import (
	"container/list"

	ibytes "github.com/cosmos/iavl/internal/bytes"
)

// Node[T] represents a node eligible for caching.
type Node[T any] interface {
	GetKey() T
}

// Cache is an in-memory structure to persist nodes for quick access.
// Please see lruCache for more details about why we need a custom
// cache implementation.
type Cache[T any] interface {
	// Adds node to cache. If full and had to remove the oldest element,
	// returns the oldest, otherwise nil.
	// CONTRACT: node can never be nil. Otherwise, cache panics.
	Add(node Node[T]) Node[T]

	// Returns Node[T] for the key, if exists. nil otherwise.
	Get(key T) Node[T]

	// Has returns true if node with key exists in cache, false otherwise.
	Has(key T) bool

	// Remove removes node with key from cache. The removed node is returned.
	// if not in cache, return nil.
	Remove(key T) Node[T]

	// Len returns the cache length.
	Len() int
}

// lruCache is an LRU cache implementation.
// The motivation for using a custom cache implementation is to
// allow for a custom max policy.
//
// Currently, the cache maximum is implemented in terms of the
// number of nodes which is not intuitive to configure.
// Instead, we are planning to add a byte maximum.
// The alternative implementations do not allow for
// customization and the ability to estimate the byte
// size of the cache.
type lruCache[K comparable, T any] struct {
	dict            map[K]*list.Element // FastNode cache.
	maxElementCount int                 // FastNode the maximum number of nodes in the cache.
	ll              *list.List          // LRU queue of cache elements. Used for deletion.
}

var _ Cache[int] = (*lruCache[string, int])(nil)

func New[K comparable, T any](maxElementCount int) Cache[T] {
	return &lruCache[K, T]{
		dict:            make(map[K]*list.Element),
		maxElementCount: maxElementCount,
		ll:              list.New(),
	}
}

func (c *lruCache[K, T]) Add(node Node[T]) Node[T] {
	key := c.getKey(node.GetKey())
	if e, exists := c.dict[key]; exists {
		c.ll.MoveToFront(e)
		old := e.Value
		e.Value = node
		return old.(Node[T])
	}

	elem := c.ll.PushFront(node)
	c.dict[key] = elem

	if c.ll.Len() > c.maxElementCount {
		oldest := c.ll.Back()
		return c.remove(oldest)
	}
	return nil
}

func (c *lruCache[K, T]) Get(key T) Node[T] {
	if ele, hit := c.dict[c.getKey(key)]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(Node[T])
	}
	return nil
}

func (c *lruCache[K, T]) Has(key T) bool {
	_, exists := c.dict[c.getKey(key)]
	return exists
}

func (c *lruCache[K, T]) Len() int {
	return c.ll.Len()
}

func (c *lruCache[K, T]) Remove(key T) Node[T] {
	if elem, exists := c.dict[c.getKey(key)]; exists {
		return c.remove(elem)
	}
	return nil
}

func (c *lruCache[K, T]) remove(e *list.Element) Node[T] {
	removed := c.ll.Remove(e).(Node[T])
	delete(c.dict, c.getKey(removed.GetKey()))
	return removed
}

func (c *lruCache[K, T]) getKey(key T) K {
	switch any(key).(type) {
	case []byte:
		return any(ibytes.UnsafeBytesToStr(any(key).([]byte))).(K)
	default:
		return any(key).(K)
	}
}
