package iavl

import (
	"sync"
)

type NodePool struct {
	syncPool *sync.Pool
	poolId   uint64
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
	return &Node{}
	//if np.poolId == math.MaxUint64 {
	//	np.poolId = 1
	//} else {
	//	np.poolId++
	//}
	//n := np.syncPool.Get().(*Node)
	//n.poolID = np.poolId
	//return n
}

func (np *NodePool) Put(node *Node) {
	return
	//node.leftNodeKey = emptyNodeKey
	//node.rightNodeKey = emptyNodeKey
	//node.rightNode = nil
	//node.leftNode = nil
	//node.nodeKey = emptyNodeKey
	//node.hash = nil
	//node.key = nil
	//node.value = nil
	//node.subtreeHeight = 0
	//node.size = 0
	//node.evict = 0
	//
	//node.poolID = 0
	//np.syncPool.Put(node)
}
