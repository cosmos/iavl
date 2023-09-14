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

	poolSize          int64
	dirtySize         int64
	maxWorkingSetSize int64

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
	}
	return frameId
}

func (np *nodePool) clockEvict() *Node {

	itr := 0
	for {
		itr++

		select {
		case cleaned := <-np.cleaned:
			for _, clean := range cleaned {
				if clean == nil {
					continue
				}
				n := np.nodes[clean.frameId]
				if n.NodeKey == clean.nk {
					np.cleanNode(n)
				}
			}
		default:
		}

		if itr > len(np.nodes)*2 {
			frameId := np.grow(len(np.nodes))
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
			n.clear()
			return n
		}
	}
}

func newNodePool(db nodeDB, size int) *nodePool {
	np := &nodePool{
		nodes:        make([]*Node, size),
		free:         make(chan int, 20_000_000),
		cleaned:      make(chan []*cleanArgs, 20_000_000),
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

	var n *Node
	if len(np.free) == 0 {
		n = np.clockEvict()
		np.poolSize -= n.varSize()
	} else {
		id := <-np.free
		n = np.nodes[id]
	}
	n.use = true
	np.dirtyNode(n)

	n.Key = key
	n.Value = value

	varSz := n.varSize()
	np.dirtySize += varSz
	np.poolSize += varSz

	return n
}

func (np *nodePool) Return(n *Node) {
	np.metrics.PoolReturn++

	np.cleanNode(n)
	np.poolSize -= n.varSize()
	np.free <- n.frameId
	n.clear()
}

func (np *nodePool) Put(n *Node) {
	np.metrics.PoolFault++

	var frameId int
	if len(np.free) == 0 {
		evicted := np.clockEvict()
		np.poolSize -= evicted.varSize()
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
}

func (np *nodePool) dirtyNode(n *Node) {
	if n.dirty {
		return
	}
	n.dirty = true
	np.dirtyCount++
}

const cleanBatch = 1000

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
