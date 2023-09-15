package iavl

import (
	"context"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
)

type checkpointArgs struct {
	set     []*Node
	delete  []NodeKey
	version int64
}

type cleanArgs struct {
	nk      NodeKey
	frameId int
}

type nodePool struct {
	db        nodeDB
	free      chan int
	nodes     []*Node
	metrics   *metrics.TreeMetrics
	clockHand int

	dirtyCount int
	lockCount  int

	poolSize       uint64
	dirtySize      uint64
	workingSize    uint64
	maxWorkingSize uint64
	timeEvicting   time.Duration

	// checkpoint
	lastCheckpoint int64
	checkpointCh   chan *checkpointArgs
	cleaned        chan []*cleanArgs
}

func (np *nodePool) grow(amount int) int {
	log.Warn().Msgf("growing node pool amount=%d; size=%d", amount, len(np.nodes)+amount)
	var frameId int
	for i := 0; i < amount; i++ {
		frameId = len(np.nodes)
		if i != amount-1 {
			np.free <- frameId
		}
		np.nodes = append(np.nodes, &Node{frameId: frameId})
		np.poolSize += nodeSize
	}
	return frameId
}

func (np *nodePool) clockEvict() *Node {

	itr := 0
	start := time.Now()
	for {
		itr++

		select {
		case cleaned := <-np.cleaned:
			for _, clean := range cleaned {
				n := np.nodes[clean.frameId]
				// inverse case: node in same frame had mutate called on it, so it's dirty in the next working set,
				// therefore don't clean.
				if n.NodeKey == clean.nk {
					np.cleanNode(n)
				}
			}
		default:
		}

		if itr > len(np.nodes)*2 {
			frameId := np.grow(len(np.nodes))
			np.timeEvicting += time.Since(start)
			return np.nodes[frameId]
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
			np.poolSize -= n.varSize()
			n.clear()
			np.timeEvicting += time.Since(start)
			return n
		}
	}
}

func newNodePool(db nodeDB, size int) *nodePool {
	np := &nodePool{
		free:         make(chan int, 20_000_000),
		cleaned:      make(chan []*cleanArgs, 20_000_000),
		db:           db,
		checkpointCh: make(chan *checkpointArgs),
	}
	np.free <- np.grow(size)
	return np
}

func (np *nodePool) Get(key, value []byte, version int64) *Node {
	np.metrics.PoolGet++

	var n *Node
	if len(np.free) == 0 {
		n = np.clockEvict()
	} else {
		id := <-np.free
		n = np.nodes[id]
	}
	n.Key = key
	n.Value = value

	if n.Value != nil {
		n._hash(version)
		n.Value = nil
	}

	n.use = true
	n.work = true
	np.dirtyNode(n)

	varSz := n.varSize()
	np.workingSize += n.sizeBytes()
	np.poolSize += varSz

	return n
}

func (np *nodePool) Return(n *Node) {
	np.metrics.PoolReturn++
	np.cleanNode(n)
	np.free <- n.frameId
	np.poolSize -= n.varSize()
	n.clear()
}

func (np *nodePool) Put(n *Node) {
	np.metrics.PoolFault++

	var frameId int
	if len(np.free) == 0 {
		evicted := np.clockEvict()
		frameId = evicted.frameId
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
	np.poolSize += n.varSize()
}

func (np *nodePool) cleanNode(n *Node) {
	if !n.dirty {
		return
	}
	n.dirty = false
	np.dirtyCount--
	np.dirtySize -= n.sizeBytes()
}

func (np *nodePool) dirtyNode(n *Node) {
	if n.dirty {
		return
	}
	n.dirty = true
	np.dirtySize += n.sizeBytes()
	np.dirtyCount++
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
			log.Info().Msgf("begin checkpoint [%d-%d]", np.lastCheckpoint, args.version)

			cleaned := make([]*cleanArgs, len(args.set))
			for i, n := range args.set {
				if err := np.db.Set(n); err != nil {
					return err
				}
				setCount++
				cleaned[i] = &cleanArgs{nk: n.NodeKey, frameId: n.frameId}
			}
			np.cleaned <- cleaned

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
