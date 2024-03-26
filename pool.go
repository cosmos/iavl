package iavl

import (
	"sync"
)

type NodePool struct {
	syncPool *sync.Pool
}

func NewNodePool() *NodePool {
	np := &NodePool{
		syncPool: &sync.Pool{
			New: func() interface{} {
				return &Node{}
			},
		},
	}
	return np
}

func (np *NodePool) Get() *Node {
	return np.syncPool.Get().(*Node)
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

	np.syncPool.Put(node)
}
