package iavl

import (
	"sync"

	"github.com/cosmos/iavl/v2/metrics"
)

type nodeCacheKey [12]byte

type NodeCache struct {
	cache     map[nodeCacheKey]*Node
	nextCache map[nodeCacheKey]*Node
	pool      sync.Pool
	nodes     []*Node

	missCount metrics.Counter
	hitCount  metrics.Counter
}

func NewNodeCache() *NodeCache {
	return &NodeCache{
		nextCache: make(map[nodeCacheKey]*Node),
		missCount: metrics.Default.NewCounter("node_cache.miss"),
		hitCount:  metrics.Default.NewCounter("node_cache.hit"),
	}
}

func (nc *NodeCache) Swap() {
	nc.cache = nc.nextCache
	nc.nextCache = make(map[nodeCacheKey]*Node)
}

func (nc *NodeCache) Get(nk *NodeKey) *Node {
	return nc.GetByKeyBytes(nk.GetKey())
}

func (nc *NodeCache) GetByKeyBytes(key []byte) *Node {
	var k nodeCacheKey
	copy(k[:], key)
	n, ok := nc.cache[k]
	if ok {
		nc.hitCount.Inc()
	} else {
		nc.missCount.Inc()
	}
	return n
}

func (nc *NodeCache) Set(node *Node) {
	var k nodeCacheKey
	copy(k[:], node.nodeKey.GetKey())
	nc.nextCache[k] = node
}
