package iavl

import (
	"sync"

	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
)

type NodeCache struct {
	cache     map[NodeKey]*Node
	nextCache map[NodeKey]*Node
	pool      sync.Pool
	nodes     []*Node

	metrics *metrics.TreeMetrics
}

func NewNodeCache(metrics *metrics.TreeMetrics) *NodeCache {
	return &NodeCache{
		nextCache: make(map[NodeKey]*Node),
		cache:     make(map[NodeKey]*Node),
		metrics:   metrics,
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
	nc.nextCache = make(map[NodeKey]*Node)
	nc.metrics.CacheHit = 0
	nc.metrics.CacheMiss = 0
}

func (nc *NodeCache) Get(nodeKey NodeKey) *Node {
	n, ok := nc.cache[nodeKey]
	if ok {
		nc.metrics.CacheHit++
	} else {
		nc.metrics.CacheMiss++
	}
	return n
}

func (nc *NodeCache) SetThis(node *Node) {
	nc.cache[node.nodeKey] = node
}

func (nc *NodeCache) Set(node *Node) {
	if len(nc.nextCache) > 10_000_000 {
		return
	}
	nc.nextCache[node.nodeKey] = node
}
