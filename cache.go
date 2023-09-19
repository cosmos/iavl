package iavl

import "sync"

type nodeCacheKey [12]byte

type NodeCache struct {
	cache     map[nodeCacheKey]*Node
	nextCache map[nodeCacheKey]*Node
	pool      sync.Pool
	nodes     []*Node
}

func NewNodeCache() *NodeCache {
	return &NodeCache{
		nextCache: make(map[nodeCacheKey]*Node),
	}
}

func (nc *NodeCache) Swap() {
	nc.cache = nc.nextCache
	nc.nextCache = make(map[nodeCacheKey]*Node)
}

func (nc *NodeCache) Get(nk *NodeKey) *Node {
	var k nodeCacheKey
	copy(k[:], nk.GetKey())
	return nc.cache[k]
}

func (nc *NodeCache) GetByKeyBytes(key []byte) *Node {
	var k nodeCacheKey
	copy(k[:], key)
	return nc.cache[k]
}

func (nc *NodeCache) Set(node *Node) {
	var k nodeCacheKey
	copy(k[:], node.nodeKey.GetKey())
	nc.nextCache[k] = node
}
