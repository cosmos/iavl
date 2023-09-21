package iavl

import (
	"sync"

	"github.com/dustin/go-humanize"
)

type nodeCacheKey [12]byte

type NodeCache struct {
	cache     map[nodeCacheKey]*Node
	nextCache map[nodeCacheKey]*Node
	pool      sync.Pool
	nodes     []*Node

	//missCount metrics.Counter
	//hitCount  metrics.Counter
	missCount int64
	hitCount  int64
}

func NewNodeCache() *NodeCache {
	return &NodeCache{
		nextCache: make(map[nodeCacheKey]*Node),
		//missCount: metrics.Default.NewCounter("node_cache.miss"),
		//hitCount:  metrics.Default.NewCounter("node_cache.hit"),
	}
}

func (nc *NodeCache) Swap() {
	l := log.With().Str("module", "node_cache").Logger()
	nc.cache, nc.nextCache = nc.nextCache, nc.cache
	l.Info().Msgf("emptying %s cache=%s",
		humanize.Comma(int64(len(nc.nextCache))),
		humanize.Comma(int64(len(nc.cache))),
	)
	for _, n := range nc.nextCache {
		nc.pool.Put(n)
	}
	nc.nextCache = make(map[nodeCacheKey]*Node)
	nc.hitCount = 0
	nc.missCount = 0
}

func (nc *NodeCache) Get(nk *NodeKey) *Node {
	return nc.GetByKeyBytes(nk.GetKey())
}

func (nc *NodeCache) GetByKeyBytes(key []byte) *Node {
	var k nodeCacheKey
	copy(k[:], key)
	n, ok := nc.cache[k]
	if ok {
		nc.hitCount++
	} else {
		nc.missCount++
	}
	return n
}

func (nc *NodeCache) Set(node *Node) {
	if len(nc.nextCache) > 10_000_000 {
		return
	}
	var k nodeCacheKey
	copy(k[:], node.nodeKey.GetKey())
	nc.nextCache[k] = node
}
