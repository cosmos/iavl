package iavl

import (
	"math"
	"sync"
)

type NodePool struct {
	syncPool *sync.Pool
	free     chan int
	nodes    []Node
	poolSize uint64

	poolId uint64
}

const initialNodePoolSize = 1_000

func NewNodePool() *NodePool {
	np := &NodePool{
		syncPool: &sync.Pool{
			New: func() interface{} {
				return &Node{}
			},
		},
		free: make(chan int, 1000),
	}
	np.grow(initialNodePoolSize)
	return np
}

func (np *NodePool) grow(amount int) {
	startSize := len(np.nodes)
	log.Warn().Msgf("growing node pool amount=%d; size=%d", amount, startSize+amount)
	for i := startSize; i < startSize+amount; i++ {
		np.free <- i
		//np.nodes = append(np.nodes, Node{poolId: i})
		np.poolSize += nodeSize
	}
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

	//return &Node{}

	//if len(np.free) == 0 {
	//	np.grow(len(np.nodes))
	//}
	//poolId := <-np.free
	//node := &np.nodes[poolId]
	//if node.hash != nil {
	//	panic("invariant violated: node hash should be nil when fetched from pool")
	//}
	//return node
}

func (np *NodePool) Put(node *Node) {
	np.resetNode(node)
	node.poolId = 0
	np.syncPool.Put(node)
	//np.free <- node.poolId
}

func (np *NodePool) resetNode(node *Node) {
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
}
