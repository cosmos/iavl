package iavl

import (
	"math"
	"sync"

	"github.com/cosmos/iavl/v2/metrics"
)

type NodePool interface {
	Get() *Node
	Put(*Node)
	Pooled() bool
}

type SyncNodePool struct {
	syncPool *sync.Pool
	metrics  metrics.Proxy
	poolId   uint64
}

type NopNodePool struct{}

func (np *NopNodePool) Get() *Node {
	return &Node{}
}

func (np *NopNodePool) Put(*Node) {}

func (np *NopNodePool) Pooled() bool { return false }

func NewNopNodePool(_ metrics.Proxy) *NopNodePool {
	return &NopNodePool{}
}

func NewSyncNodePool(metrics metrics.Proxy) *SyncNodePool {
	np := &SyncNodePool{
		syncPool: &sync.Pool{
			New: func() interface{} {
				return &Node{}
			},
		},
		metrics: metrics,
	}
	return np
}

func (np *SyncNodePool) Pooled() bool { return true }

func (np *SyncNodePool) Get() *Node {
	np.metrics.IncrCounter(1, metricsNamespace, "pool_get")
	if np.poolId == math.MaxUint64 {
		np.poolId = 1
	} else {
		np.poolId++
	}
	n := np.syncPool.Get().(*Node)
	n.poolID = np.poolId
	return n
}

func (np *SyncNodePool) Put(node *Node) {
	np.metrics.IncrCounter(1, metricsNamespace, "pool_return")
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
	node.evict = 0
	node.poolID = 0
	np.syncPool.Put(node)
}
