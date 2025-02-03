package iavl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

var (
	log = zlog.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.Stamp,
	})
	leafSequenceStart = uint32(1 << 31)
)

type nodeDelete struct {
	// the sequence in which this deletion was processed
	deleteKey NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	leafKey []byte
}

type writeQueue struct {
	branches      []*Node
	leaves        []*Node
	branchOrphans []NodeKey
	leafOrphans   []NodeKey
	deletes       []*nodeDelete
}

type Tree struct {
	version       int64
	stagedVersion int64

	previousRoot *Node
	root         *Node
	stagedRoot   *Node

	metrics          *metrics.TreeMetrics
	sql              *SqliteDb
	pool             *NodePool
	checkpoints      *VersionRange
	shouldCheckpoint bool

	// options
	maxWorkingSize     uint64
	workingBytes       uint64
	checkpointInterval int64
	checkpointMemory   uint64
	workingSize        int64
	storeLeafValues    bool
	storeLatestLeaves  bool
	heightFilter       int8
	metricsProxy       metrics.Proxy

	// pruning config
	orphanBranchCount   int64
	orphanLeafCount     int64
	pruneRatio          float64
	minimumKeepVersions int64
	pruneTo             int64

	// state
	*writeQueue
	evictions      []*Node
	branchSequence uint32
	leafSequence   uint32
	evictionDepth  int8

	// synchronize h and h-1 reads to SaveVersion
	versionLock sync.RWMutex
	// used during replay to save on hashing leaf nodes
	replayHash []byte
}

type TreeOptions struct {
	// CheckpointInterval is the number of versions between each checkpoint.
	// In general, a higher value trades performance for longer replay times and space efficiency.
	CheckpointInterval int64

	// StateStorage is a boolean flag that determines whether to store the leaf values in the tree.
	// Default true.
	StateStorage bool

	// When greater than 0, the tree will evict leaf nodes from memory at each SaveVersion call.
	// When less than 0, the tree will never evict leaf nodes from memory, trading memory for performance.
	HeightFilter int8

	// EvictionDepth defines the depth of the tree at which eviction will occur on checkpoints.
	EvictionDepth int8

	// MetricsProxy is a metrics.Proxy that will be used to measure the performance of the tree.
	MetricsProxy metrics.Proxy

	// PruneRatio is the ratio of orphaned nodes to total nodes at which pruning will occur.
	// Default 1.
	PruneRatio float64

	// MinimumKeepVersions is the minimum number of versions to keep in the tree.
	MinimumKeepVersions int64

	checkpointMemory uint64
}

func DefaultTreeOptions() TreeOptions {
	return TreeOptions{
		CheckpointInterval:  1000,
		StateStorage:        true,
		HeightFilter:        1,
		EvictionDepth:       -1,
		PruneRatio:          1,
		MinimumKeepVersions: 100,
	}
}

func NewTree(sql *SqliteDb, pool *NodePool, opts TreeOptions) *Tree {
	tree := &Tree{
		sql:                 sql,
		pool:                pool,
		checkpoints:         &VersionRange{},
		metrics:             &metrics.TreeMetrics{},
		maxWorkingSize:      1.5 * 1024 * 1024 * 1024,
		checkpointInterval:  opts.CheckpointInterval,
		checkpointMemory:    opts.checkpointMemory,
		storeLeafValues:     opts.StateStorage,
		minimumKeepVersions: opts.MinimumKeepVersions,
		pruneRatio:          opts.PruneRatio,
		storeLatestLeaves:   false,
		heightFilter:        opts.HeightFilter,
		metricsProxy:        opts.MetricsProxy,
		evictionDepth:       opts.EvictionDepth,
		writeQueue:          &writeQueue{},
		version:             0,
		stagedVersion:       1,
	}
	tree.resetSequence()
	if tree.minimumKeepVersions < 1 {
		tree.minimumKeepVersions = 1
	}

	return tree
}

func (tree *Tree) ReadonlyClone() (*Tree, error) {
	sqlOpts := tree.sql.opts
	sqlOpts.Readonly = true
	sql, err := NewSqliteDb(tree.pool, sqlOpts)
	if err != nil {
		return nil, err
	}
	return &Tree{
		sql:                sql,
		pool:               tree.pool,
		checkpoints:        &VersionRange{},
		metrics:            &metrics.TreeMetrics{},
		maxWorkingSize:     tree.maxWorkingSize,
		checkpointInterval: tree.checkpointInterval,
		checkpointMemory:   tree.checkpointMemory,
		storeLeafValues:    tree.storeLeafValues,
		storeLatestLeaves:  tree.storeLatestLeaves,
		heightFilter:       tree.heightFilter,
		metricsProxy:       tree.metricsProxy,
		evictionDepth:      tree.evictionDepth,
		writeQueue:         &writeQueue{},
	}, nil
}

func (tree *Tree) LoadVersion(version int64) (err error) {
	if tree.sql == nil {
		return fmt.Errorf("sql is nil")
	}

	tree.workingBytes = 0
	tree.workingSize = 0

	tree.checkpoints, err = tree.sql.loadCheckpointRange()
	if err != nil {
		return err
	}
	// initial version case; permit loading an arbitrary version if there are no checkpoints
	if tree.checkpoints.Len() == 0 {
		tree.version = version
		tree.stagedVersion = version + 1
		return nil
	}
	tree.version = tree.checkpoints.FindPrevious(version)
	if tree.version == -1 {
		return fmt.Errorf("version %d too old; prior checkpoint not found", version)
	}
	tree.checkpoints.TruncateTo(tree.version)

	tree.root, err = tree.sql.LoadRoot(tree.version)
	if err != nil {
		return err
	}
	tree.stagedRoot = tree.root
	tree.pruneTo, tree.orphanBranchCount, tree.orphanLeafCount, err = tree.sql.getCheckpointCounts(tree.version)
	if err != nil {
		return err
	}

	if version > tree.version {
		var targetHash []byte
		targetRoot, err := tree.sql.LoadRoot(version)
		if err != nil {
			return err
		}
		if targetRoot == nil {
			targetHash = emptyHash
		} else {
			targetHash = targetRoot.hash
		}

		if err = tree.sql.replayChangelog(tree, version, targetHash); err != nil {
			return err
		}
	}

	tree.stagedVersion = tree.version + 1
	tree.sql.writeConn, err = tree.sql.newWriteConnection(tree.stagedVersion)
	return err
}

func (tree *Tree) LoadSnapshot(version int64, traverseOrder TraverseOrderType) (err error) {
	var v int64
	tree.root, v, err = tree.sql.ImportMostRecentSnapshot(version, traverseOrder, true)
	if err != nil {
		return err
	}
	tree.stagedRoot = tree.root
	if v < version {
		return fmt.Errorf("requested %d found snapshot %d, replay not yet supported", version, v)
	}
	tree.version = v
	tree.stagedVersion = v + 1
	tree.checkpoints, err = tree.sql.loadCheckpointRange()
	return err
}

func (tree *Tree) SaveSnapshot() (err error) {
	ctx := context.Background()
	return tree.sql.Snapshot(ctx, tree)
}

// SaveVersion saves the staged version of the tree to storage. It returns the hash of the root node.
// Not thread-safe.
func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	var err error
	if err = tree.sql.closeHangingIterators(); err != nil {
		return nil, 0, err
	}

	isInitialVersion := tree.checkpoints.Len() == 0
	if !tree.shouldCheckpoint {
		tree.shouldCheckpoint = isInitialVersion ||
			(tree.checkpointInterval > 0 && tree.stagedVersion-tree.checkpoints.Last() >= tree.checkpointInterval) ||
			(tree.checkpointMemory > 0 && tree.workingBytes >= tree.checkpointMemory)
	}
	rootHash := tree.computeHash()

	if isInitialVersion {
		if err := tree.sql.createShard(tree.stagedVersion, false); err != nil {
			return nil, tree.version, err
		}
	}
	if err := tree.sql.ensureWriteConnection(); err != nil {
		return nil, tree.version, err
	}

	batch := &sqliteBatch{
		conn:              tree.sql.writeConn,
		queue:             tree.writeQueue,
		version:           tree.stagedVersion,
		size:              200_000,
		logger:            tree.sql.logger,
		storeLatestLeaves: tree.storeLatestLeaves,
	}
	// save
	saveStart := time.Now()
	if err := batch.save(); err != nil {
		return nil, tree.version, err
	}
	if err := tree.sql.SaveRoot(tree.stagedVersion, tree.stagedRoot, tree.shouldCheckpoint); err != nil {
		return nil, tree.version, fmt.Errorf("save root failed: %w", err)
	}

	if err := tree.postCheckpoint(); err != nil {
		return nil, tree.version, fmt.Errorf("postCheckpoint failed: %w", err)
	}

	dur := time.Since(saveStart)
	tree.sql.metrics.WriteDurations = append(tree.sql.metrics.WriteDurations, dur)
	tree.sql.metrics.WriteTime += dur
	tree.sql.metrics.WriteLeaves += int64(len(tree.leaves))

	// now that nodes have been written to storage, we can evict flagged nodes from memory
	if err := tree.evictNodes(); err != nil {
		return nil, tree.version, err
	}

	tree.leafOrphans = nil
	tree.leaves = nil
	tree.branches = nil
	tree.deletes = nil
	tree.resetSequence()

	tree.versionLock.Lock()
	// prune
	if err := tree.sql.checkPruning(); err != nil {
		return nil, tree.version, fmt.Errorf("pruning failed: %w", err)
	}
	if tree.shouldCheckpoint && tree.pruneTo != 0 {
		if tree.stagedVersion-tree.pruneTo > tree.minimumKeepVersions {
			tree.sql.beginPrune(tree.pruneTo)
			tree.pruneTo = 0
		}
	}

	tree.shouldCheckpoint = false
	tree.previousRoot = tree.root
	tree.root = tree.stagedRoot
	tree.version++
	tree.stagedVersion++
	tree.versionLock.Unlock()

	return rootHash, tree.version, nil
}

func (tree *Tree) postCheckpoint() error {
	if !tree.shouldCheckpoint {
		return nil
	}
	if err := tree.sql.writeConn.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}
	if err := tree.checkpoints.Add(tree.stagedVersion); err != nil {
		return err
	}
	tree.branchOrphans = nil

	var size float64
	if tree.stagedRoot == nil {
		size = 0
	} else {
		size = float64(tree.stagedRoot.size * 2)
	}
	// branches + leaves:
	pruneRatio := float64(tree.orphanBranchCount+tree.orphanLeafCount) / size
	tree.sql.logger.Info().
		Float64("pruneRatio", pruneRatio).
		Int64("branchCount", tree.orphanBranchCount).
		Int64("leafCount", tree.orphanLeafCount).
		Int64("pruneTo", tree.pruneTo).
		Msg("prune check")

	var planPune bool
	if tree.pruneRatio != 0 && pruneRatio > tree.pruneRatio && tree.pruneTo == 0 {
		// plan a prune in minimumKeepVersions
		tree.pruneTo = tree.stagedVersion
		planPune = true
	}
	if err := tree.sql.updateShard(tree.stagedVersion, tree.pruneTo, tree.orphanBranchCount, tree.orphanLeafCount); err != nil {
		return err
	}
	if !planPune {
		return nil
	}

	tree.sql.logger.Info().
		Int64("orphans", tree.orphanBranchCount).
		Int64("size", tree.stagedRoot.size).
		Str("pruneRatio", fmt.Sprintf("%.3f", pruneRatio)).
		Int64("version", tree.stagedVersion+1).
		Int64("pruneTo", tree.stagedVersion).
		Msg("create shard")

	if err := tree.sql.createShard(tree.stagedVersion+1, true); err != nil {
		return err
	}
	if err := tree.sql.resetWriteConnection(); err != nil {
		return err
	}
	tree.orphanBranchCount = 0
	tree.orphanLeafCount = 0
	return nil
}

func (tree *Tree) evictNodes() error {
	for i, node := range tree.evictions {
		switch node.evict {
		case 1:
			tree.returnNode(node.leftNode)
			node.leftNode = nil
		case 2:
			tree.returnNode(node.rightNode)
			node.rightNode = nil
		case 3:
			tree.returnNode(node.leftNode)
			node.leftNode = nil
			tree.returnNode(node.rightNode)
			node.rightNode = nil
		default:
			return fmt.Errorf("unexpected eviction flag %d i=%d", node.evict, i)
		}
		node.evict = 0
	}
	tree.evictions = nil
	return nil
}

// ComputeHash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the tree root node hash.
// If the tree is empty (i.e. the node is nil), returns the hash of an empty input,
// to conform with RFC-6962.
func (tree *Tree) computeHash() []byte {
	if tree.stagedRoot == nil {
		return sha256.New().Sum(nil)
	}
	tree.deepHash(tree.stagedRoot, 0)
	return tree.stagedRoot.hash
}

func (tree *Tree) deepHash(node *Node, depth int8) {
	if node == nil {
		panic(fmt.Sprintf("node is nil; sql.path=%s", tree.sql.opts.Path))
	}
	if node.isLeaf() {
		// new leaves are written every version
		if node.Version() == tree.stagedVersion {
			tree.leaves = append(tree.leaves, node)
		}

		// always end recursion at a leaf
		return
	}

	if node.hash == nil {
		// When the child is a leaf, this will initiate a leafRead from storage for the sole purpose of producing a hash.
		// Recall that a terminal tree node may have only updated one leaf this version.
		// We can explore storing right/left hash in terminal tree nodes to avoid this, or changing the storage
		// format to iavl v0 where left/right hash are stored in the node.
		tree.deepHash(node.left(tree.sql, tree.sql.hotConnectionFactory), depth+1)
		tree.deepHash(node.right(tree.sql, tree.sql.hotConnectionFactory), depth+1)
		node._hash()
	} else if tree.shouldCheckpoint {
		// when checkpointing traverse the entire tree to accumulate dirty branches and flag for eviction
		// even if the node hash is already computed
		if node.leftNode != nil {
			tree.deepHash(node.leftNode, depth+1)
		}
		if node.rightNode != nil {
			tree.deepHash(node.rightNode, depth+1)
		}
	}

	// leaf node eviction occurs every version
	if tree.heightFilter > 0 {
		novel := node.evict == 0
		if node.leftNode != nil && node.leftNode.isLeaf() {
			node.evict++
		}
		if node.rightNode != nil && node.rightNode.isLeaf() {
			node.evict += 2
		}
		if novel && node.evict > 0 {
			tree.evictions = append(tree.evictions, node)
		}
	}

	if tree.shouldCheckpoint {
		if node.Version() > tree.checkpoints.Last() {
			tree.branches = append(tree.branches, node)
		}
		// full tree eviction occurs only during checkpoints
		if depth >= tree.evictionDepth {
			novel := node.evict == 0
			if node.leftNode != nil && node.evict%2 == 0 {
				node.evict++
			}
			if node.rightNode != nil && node.evict < 2 {
				node.evict += 2
			}
			if novel && node.evict > 0 {
				tree.evictions = append(tree.evictions, node)
			}
		}
	}
}

func (tree *Tree) getRecentRoot(version int64) (bool, *Node) {
	tree.versionLock.RLock()
	defer tree.versionLock.RUnlock()
	var root Node

	switch version {
	case tree.version:
		if tree.root == nil {
			return true, nil
		}
		root = *tree.root
	case tree.version - 1:
		if tree.previousRoot == nil {
			return true, nil
		}
		root = *tree.previousRoot
	default:
		return false, nil
	}
	return true, &root
}

func (tree *Tree) connectionFactory() connectionFactory {
	if tree.sql == nil {
		return newReadConnectionFactory(nil)
	}
	return tree.sql.hotConnectionFactory
}

func (tree *Tree) GetRecent(version int64, key []byte) (bool, []byte, error) {
	ok, root := tree.getRecentRoot(version)
	if !ok {
		return false, nil, nil
	}
	if root == nil {
		return true, nil, nil
	}
	_, res, err := root.get(tree, tree.connectionFactory(), key)
	return true, res, err
}

func (tree *Tree) Get(key []byte) ([]byte, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), "iavl_v2", "get")
	}
	var (
		res []byte
		err error
	)
	if tree.storeLatestLeaves {
		res, err = tree.sql.GetLatestLeaf(key)
	} else {
		if tree.root == nil {
			return nil, nil
		}
		_, res, err = tree.root.get(tree, tree.connectionFactory(), key)
	}
	return res, err
}

func (tree *Tree) GetWithIndex(key []byte) (int64, []byte, error) {
	if tree.root == nil {
		return 0, nil, nil
	}
	return tree.root.get(tree, tree.connectionFactory(), key)
}

func (tree *Tree) GetByIndex(index int64) (key []byte, value []byte, err error) {
	if tree.root == nil {
		return nil, nil, nil
	}
	return tree.getByIndex(tree.root, index, tree.connectionFactory())
}

func (tree *Tree) getByIndex(node *Node, index int64, cf connectionFactory) (key []byte, value []byte, err error) {
	if node.isLeaf() {
		if index == 0 {
			return node.key, node.value, nil
		}
		return nil, nil, nil
	}

	leftNode, err := node.getLeftNode(tree.sql, cf)
	if err != nil {
		return nil, nil, err
	}

	if index < leftNode.size {
		return tree.getByIndex(leftNode, index, cf)
	}

	rightNode, err := node.getRightNode(tree.sql, cf)
	if err != nil {
		return nil, nil, err
	}

	return tree.getByIndex(rightNode, index-leftNode.size, cf)
}

func (tree *Tree) Has(key []byte) (bool, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), "iavl_v2", "has")
	}
	var (
		err error
		val []byte
	)
	if tree.storeLatestLeaves {
		val, err = tree.sql.GetLatestLeaf(key)
	} else {
		if tree.root == nil {
			return false, nil
		}
		_, val, err = tree.root.get(tree, tree.connectionFactory(), key)
	}
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

// Set sets a key in the working tree. Nil values are invalid. The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *Tree) Set(key, value []byte) (updated bool, err error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), "iavl_v2", "set")
	}
	updated, err = tree.set(key, value, tree.connectionFactory())
	if err != nil {
		return false, err
	}
	if updated {
		tree.metrics.TreeUpdate++
	} else {
		tree.metrics.TreeNewNode++
	}
	return updated, nil
}

func (tree *Tree) set(key []byte, value []byte, cf connectionFactory) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.stagedRoot == nil {
		tree.stagedRoot = tree.NewLeafNode(key, value)
		return updated, nil
	}

	tree.stagedRoot, updated, err = tree.recursiveSet(tree.stagedRoot, key, value, cf)
	return updated, err
}

func (tree *Tree) recursiveSet(node *Node, key, value []byte, cf connectionFactory) (
	newSelf *Node, updated bool, err error,
) {
	if node == nil {
		panic("node is nil")
	}
	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1: // setKey < leafKey
			tree.metrics.PoolGet += 2
			parent := tree.pool.Get()
			parent.nodeKey = tree.nextNodeKey()
			parent.key = node.key
			parent.subtreeHeight = 1
			parent.size = 2
			parent.setLeft(tree.NewLeafNode(key, value))
			parent.setRight(node)

			tree.workingBytes += parent.sizeBytes()
			tree.workingSize++
			return parent, false, nil
		case 1: // setKey > leafKey
			tree.metrics.PoolGet += 2
			parent := tree.pool.Get()
			parent.nodeKey = tree.nextNodeKey()
			parent.key = key
			parent.subtreeHeight = 1
			parent.size = 2
			parent.setLeft(node)
			parent.setRight(tree.NewLeafNode(key, value))

			tree.workingBytes += parent.sizeBytes()
			tree.workingSize++
			return parent, false, nil
		default:
			tree.addOrphan(node)
			wasDirty := node.isDirty(tree)
			node = tree.stageNode(node)
			if wasDirty {
				tree.workingBytes -= node.sizeBytes()
			}
			node.value = value
			if tree.replayHash != nil {
				node.hash = tree.replayHash
			}
			node._hash()
			if !tree.storeLeafValues {
				node.value = nil
			}
			tree.workingBytes += node.sizeBytes()
			return node, true, nil
		}
	} else {
		tree.addOrphan(node)
		node = tree.stageNode(node)

		var child *Node
		if bytes.Compare(key, node.key) < 0 {
			child, updated, err = tree.recursiveSet(node.left(tree.sql, cf), key, value, cf)
			if err != nil {
				return nil, updated, err
			}
			node.setLeft(child)
		} else {
			child, updated, err = tree.recursiveSet(node.right(tree.sql, cf), key, value, cf)
			if err != nil {
				return nil, updated, err
			}
			node.setRight(child)
		}

		if updated {
			return node, updated, nil
		}
		err = node.calcHeightAndSize(tree, cf)
		if err != nil {
			return nil, false, err
		}
		newNode, err := tree.balance(node, cf)
		if err != nil {
			return nil, false, err
		}
		return newNode, updated, err
	}
}

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.metricsProxy != nil {
		tree.metricsProxy.MeasureSince(time.Now(), "iavL_v2", "remove")
	}
	if tree.stagedRoot == nil {
		return nil, false, nil
	}
	return tree.remove(key, tree.connectionFactory())
}

func (tree *Tree) remove(key []byte, cf connectionFactory) ([]byte, bool, error) {
	if tree.stagedRoot == nil {
		return nil, false, nil
	}
	newRoot, _, value, removed, err := tree.recursiveRemove(tree.stagedRoot, key, cf)
	if err != nil {
		return nil, false, err
	}
	if !removed {
		return nil, false, nil
	}
	tree.metrics.TreeDelete++
	tree.stagedRoot = newRoot
	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
func (tree *Tree) recursiveRemove(node *Node, key []byte, cf connectionFactory) (
	newSelf *Node, newKey, newValue []byte, removed bool, err error,
) {
	if node.isLeaf() {
		if bytes.Equal(key, node.key) {
			tree.addOrphan(node)
			tree.addDelete(node)
			tree.returnNode(node)
			return nil, nil, node.value, true, nil
		}
		return node, nil, nil, false, nil
	}

	// node.key < key; we go to the left to find the key:
	if bytes.Compare(key, node.key) < 0 {
		newLeftNode, newKey, value, removed, err := tree.recursiveRemove(node.left(tree.sql, cf), key, cf)
		if err != nil {
			return nil, nil, nil, false, err
		}

		if !removed {
			return node, nil, value, removed, nil
		}

		tree.addOrphan(node)

		// left node held value, was removed
		// collapse `node.rightNode` into `node`
		if newLeftNode == nil {
			right := node.right(tree.sql, cf)
			k := node.key
			tree.returnNode(node)
			return right, k, value, removed, nil
		}

		node = tree.stageNode(node)

		node.setLeft(newLeftNode)
		err = node.calcHeightAndSize(tree, cf)
		if err != nil {
			return nil, nil, nil, false, err
		}
		node, err = tree.balance(node, cf)
		if err != nil {
			return nil, nil, nil, false, err
		}

		return node, newKey, value, removed, nil
	}
	// node.key >= key; either found or look to the right:
	newRightNode, newKey, value, removed, err := tree.recursiveRemove(node.right(tree.sql, cf), key, cf)
	if err != nil {
		return nil, nil, nil, false, err
	}

	if !removed {
		return node, nil, value, removed, nil
	}

	tree.addOrphan(node)

	// right node held value, was removed
	// collapse `node.leftNode` into `node`
	if newRightNode == nil {
		left := node.left(tree.sql, cf)
		tree.returnNode(node)
		return left, nil, value, removed, nil
	}

	node = tree.stageNode(node)

	node.setRight(newRightNode)
	if newKey != nil {
		node.key = newKey
	}
	err = node.calcHeightAndSize(tree, cf)
	if err != nil {
		return nil, nil, nil, false, err
	}

	node, err = tree.balance(node, cf)
	if err != nil {
		return nil, nil, nil, false, err
	}

	return node, nil, value, removed, nil
}

func (tree *Tree) Size() int64 {
	return tree.root.size
}

func (tree *Tree) Height() int8 {
	return tree.root.subtreeHeight
}

func (tree *Tree) nextNodeKey() NodeKey {
	tree.branchSequence++
	if tree.branchSequence > leafSequenceStart {
		panic("branch sequence overflow")
	}
	nk := NewNodeKey(tree.stagedVersion, tree.branchSequence)
	return nk
}

func (tree *Tree) nextLeafNodeKey() NodeKey {
	tree.leafSequence++
	if tree.leafSequence < leafSequenceStart {
		panic("leaf sequence underflow")
	}
	nk := NewNodeKey(tree.stagedVersion, tree.leafSequence)
	return nk
}

func (tree *Tree) resetSequence() {
	tree.branchSequence = 0
	tree.leafSequence = leafSequenceStart
}

func (tree *Tree) mutateNode(node *Node) {
	// this second conditional is only relevant in replay; or more specifically, in cases where hashing has been
	// deferred between versions
	if node.hash == nil && node.Version() == tree.version+1 {
		return
	}
	node.hash = nil
	node.nodeKey = tree.nextNodeKey()

	if node.isDirty(tree) {
		return
	}

	tree.workingSize++
	if !node.isLeaf() {
		tree.workingBytes += node.sizeBytes()
	}
}

// TODO
// during replay stageNode should mutate in place, i.e. probably call mutateNode
// in order to save GC pressure

func (tree *Tree) stageNode(node *Node) *Node {
	if node.isDirty(tree) {
		if node.hash != nil {
			// TODO debug and write a comment here explaining this weird code path
			// how can node be dirty but have a non nil hash?
			// is this only leaf nodes?
			node.hash = nil
			if node.isLeaf() {
				node.nodeKey = tree.nextLeafNodeKey()
			} else {
				node.nodeKey = tree.nextNodeKey()
			}
		}
		return node
	}
	clone := node.clone(tree)
	tree.workingSize++
	if !node.isLeaf() {
		tree.workingBytes += node.sizeBytes()
	}
	return clone
}

func (tree *Tree) addOrphan(node *Node) {
	if !node.isLeaf() && node.Version() <= tree.checkpoints.Last() {
		tree.branchOrphans = append(tree.branchOrphans, node.nodeKey)
		tree.orphanBranchCount++
	} else if node.isLeaf() && !node.isDirty(tree) {
		tree.leafOrphans = append(tree.leafOrphans, node.nodeKey)
		tree.orphanLeafCount++
	}
}

func (tree *Tree) addDelete(node *Node) {
	// added and removed in the same version; no op.
	if node.Version() == tree.version+1 {
		return
	}
	del := &nodeDelete{
		deleteKey: tree.nextLeafNodeKey(),
		leafKey:   node.key,
	}
	tree.deletes = append(tree.deletes, del)
}

// NewLeafNode returns a new node from a key, value and version.
func (tree *Tree) NewLeafNode(key []byte, value []byte) *Node {
	node := tree.pool.Get()

	node.nodeKey = tree.nextLeafNodeKey()

	node.key = key
	node.subtreeHeight = 0
	node.size = 1

	node.value = value
	if tree.replayHash != nil {
		node.hash = tree.replayHash
	}
	node._hash()
	if !tree.storeLeafValues {
		node.value = nil
	}

	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
	return node
}

func (tree *Tree) returnNode(node *Node) {
	if node.isDirty(tree) {
		tree.workingBytes -= node.sizeBytes()
		tree.workingSize--
	}
	tree.pool.Put(node)
}

func (tree *Tree) Close() error {
	return tree.sql.Close()
}

func (tree *Tree) Hash() []byte {
	if tree.root == nil {
		return emptyHash
	}
	return tree.root.hash
}

func (tree *Tree) Version() int64 {
	return tree.version
}

func (tree *Tree) WriteLatestLeaves() (err error) {
	return tree.sql.WriteLatestLeaves(tree)
}

func (tree *Tree) DeleteVersionsTo(_ int64) error {
	return nil
}

func (tree *Tree) WorkingBytes() uint64 {
	return tree.workingBytes
}

func (tree *Tree) SetShouldCheckpoint() {
	tree.shouldCheckpoint = true
}

func (tree *Tree) SetInitialVersion(version int64) error {
	tree.stagedVersion = version
	tree.version = version - 1
	var err error
	tree.checkpoints, err = tree.sql.loadCheckpointRange()
	return err
}

func (tree *Tree) Import(version int64) (*Importer, error) {
	return newImporter(tree, version)
}
