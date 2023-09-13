package iavl

import (
	"context"
	"fmt"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
)

type checkpointArgs struct {
	overflow []*Node
	set      []*Node
	delete   []NodeKey
	version  int64
}

type nodePool struct {
	db        nodeDB
	free      chan int
	nodes     []*Node
	metrics   *metrics.TreeMetrics
	clockHand int

	dirtyCount int
	lockCount  int

	poolSize  int64
	dirtySize int64

	// checkpoint
	lastCheckpoint int64
	checkpointCh   chan *checkpointArgs
}

func (np *nodePool) clockEvict() *Node {
	itr := 0
	for {
		itr++
		if itr > len(np.nodes)*2 {
			panic("eviction failed, pool exhausted")
		}

		n := np.nodes[np.clockHand]
		np.clockHand++
		if np.clockHand == len(np.nodes) {
			np.clockHand = 0
		}

		switch {
		case n.use:
			// always clear the use bit, dirty nodes included.
			np.metrics.PoolEvictMiss++
			n.use = false
			continue
		case n.dirty:
			// never evict dirty nodes
			np.metrics.PoolEvictMiss++
			continue
		default:
			np.metrics.PoolEvict++
			n.clear()
			return n
		}
	}
}

func newNodePool(db nodeDB, size int) *nodePool {
	np := &nodePool{
		nodes:        make([]*Node, size),
		free:         make(chan int, size),
		db:           db,
		checkpointCh: make(chan *checkpointArgs),
	}
	for i := 0; i < size; i++ {
		np.free <- i
		np.nodes[i] = &Node{frameId: i}
		np.poolSize += nodeSize
	}
	return np
}

func (np *nodePool) Get(key, value []byte) *Node {
	np.metrics.PoolGet++

	// TODO
	// soft ceiling: test/configure different fractions
	if np.dirtyCount > len(np.nodes)/2 {
		np.metrics.PoolDirtyOverflow++
		// allocate a new node. it will be discarded on next flush
		n := &Node{overflow: true}
		return n
	}

	var n *Node
	if len(np.free) == 0 {
		n = np.clockEvict()
	} else {
		id := <-np.free
		n = np.nodes[id]
	}
	n.use = true
	np.dirtyNode(n)
	n.Key = key
	n.Value = value
	varSz := n.varSize()
	np.poolSize += varSz
	np.dirtySize += varSz

	return n
}

func (np *nodePool) Return(n *Node) {
	if n.overflow {
		// this un-managed node had `mutateNode` called on it
		if n.dirty {
			np.dirtyCount--
		}

		// overflow nodes are not managed
		return
	}
	np.free <- n.frameId
	np.metrics.PoolReturn++
	n.clear()
	np.cleanNode(n)
}

func (np *nodePool) Put(n *Node) {
	np.metrics.PoolFault++
	var frameId int
	if len(np.free) == 0 {
		frameId = np.clockEvict().frameId
	} else {
		frameId = <-np.free
	}
	// replace node in page cache. presumably n was fetched and unmarshalled from an external source.
	// an optimization may be unmarshalling directly into the page cache.
	np.nodes[frameId] = n
	if n == nil {
		panic("nodePool.Put() with nil node")
	}
	n.frameId = frameId
	n.use = true
}

func (np *nodePool) FlushNode(n *Node) {
	// TODO errors
	switch {
	case n.dirty:
		err := np.db.Set(n)
		if err != nil {
			panic(err)
		}
		np.cleanNode(n)
	case n.overflow:
		err := np.db.Set(n)
		if err != nil {
			panic(err)
		}
	default:
		panic("strange, flushing a clean node")
	}
}

func (np *nodePool) cleanNode(n *Node) {
	if !n.dirty {
		return
	}
	n.dirty = false
	np.dirtyCount--
}

func (np *nodePool) dirtyNode(n *Node) {
	if n.dirty {
		return
	}
	n.dirty = true
	np.dirtyCount++
}

func (np *nodePool) lockDirty() {
	for _, n := range np.nodes {
		if n.dirty {
			n.lock = true
			np.lockCount++
		}
	}
}

func (np *nodePool) unlockDirty() error {
	lockCount := 0
	for _, n := range np.nodes {
		if n.lock {
			n.lock = false
			lockCount++
		}
	}
	if lockCount != np.lockCount {
		return fmt.Errorf("lock count mismatch: %d != %d", lockCount, np.lockCount)
	}
	np.lockCount = 0
	return nil
}

func (np *nodePool) checkpoint(overflow []*Node) error {
	for _, n := range np.nodes {
		if n.dirty {
			np.FlushNode(n)
		}
	}
	for _, n := range overflow {
		np.FlushNode(n)
	}
	//if np.dirtyCount != 0 {
	//	return fmt.Errorf("dirty count mismatch: %d != 0", np.dirtyCount)
	//}
	return nil
}

func (np *nodePool) CheckpointRunner(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("node pool checkpoint runner exiting")
			return nil
		case args := <-np.checkpointCh:
			if args.version == -1 {
				continue
			}
			start := time.Now()
			var setCount, deleteCount int
			log.Info().Msgf("checkpoint [%d-%d]", np.lastCheckpoint, args.version)
			for _, n := range args.set {
				if err := np.db.Set(n); err != nil {
					return err
				}
				setCount++
			}
			for _, nk := range args.delete {
				if err := np.db.Delete(nk); err != nil {
					return err
				}
				deleteCount++
			}
			log.Info().Msgf("checkpoint [%d-%d] complete. sets: %d, deletes: %d, elapsed: %s",
				np.lastCheckpoint, args.version, setCount, deleteCount, time.Since(start))
			np.lastCheckpoint = args.version
		}
	}
}
