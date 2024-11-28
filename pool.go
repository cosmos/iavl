package iavl

import (
	"math"
	"sync"
)

type NodePool struct {
	syncPool *sync.Pool

	free   chan int
	poolId uint64
}

func NewNodePool() *NodePool {
	np := &NodePool{
		syncPool: &sync.Pool{
			New: func() interface{} {
				return &Node{}
			},
		},
		free: make(chan int, 1000),
	}
	return np
}

func (np *NodePool) Get() *Node {
	if np.poolId == math.MaxUint64 {
		np.poolId = 1
	} else {
		np.poolId++
	}
	n := np.syncPool.Get().(*Node)
	n.poolId = np.poolId
	return n
}

func (np *NodePool) Put(node *Node) {
	node.leftNodeKey = emptyNodeKey
	node.rightNodeKey = emptyNodeKey
	node.rightNode = nil
	node.leftNode = nil
	node.nodeKey = emptyNodeKey
	node.hash = nil
	node.key = nil
	node.value = nil
	node.subtreeHeight = 0
	node.size = 0
	node.dirty = false
	node.evict = false

	node.poolId = 0
	np.syncPool.Put(node)
}

func (np *NodePool) clone(n *Node) *Node {
	node := np.Get()
	node.leftNodeKey = n.leftNodeKey
	node.rightNodeKey = n.rightNodeKey
	node.nodeKey = n.nodeKey
	node.hash = n.hash
	node.key = n.key
	node.value = n.value
	node.subtreeHeight = n.subtreeHeight
	node.size = n.size
	return node
}
