package iavl

import "sync"

type nodePool struct {
	pool *sync.Pool
}

func newNodePool() *nodePool {
	return &nodePool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &Node{}
			},
		},
	}
}

func (np *nodePool) Get() *Node {
	node := np.pool.Get().(*Node)
	node.leftNodeKey = nil
	node.rightNodeKey = nil
	node.rightNode = nil
	node.leftNode = nil
	node.nodeKey = nil
	node.hash = nil
	node.key = nil
	node.value = nil
	node.subtreeHeight = 0
	node.size = 0
	node.dirty = false
	return node
}

func (np *nodePool) Put(node *Node) {
	np.pool.Put(node)
}
