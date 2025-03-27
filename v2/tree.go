package iavl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
)

const (
	metricsNamespace  = "iavl_v2"
	leafSequenceStart = uint32(1 << 31)
)

type nodeDelete struct {
	// the sequence in which this deletion was processed
	deleteKey NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	leafKey []byte
}

type Tree struct {
	version      int64
	root         *Node
	metrics      metrics.Proxy
	sql          *SqliteDb
	sqlWriter    *sqlWriter
	writerCancel context.CancelFunc
	pool         *NodePool

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

	// state
	branches       []*Node
	leaves         []*Node
	branchOrphans  []NodeKey
	leafOrphans    []NodeKey
	deletes        []*nodeDelete
	leafSequence   uint32
	branchSequence uint32
	isReplaying    bool
	evictionDepth  int8
}

type TreeOptions struct {
	CheckpointInterval int64
	CheckpointMemory   uint64
	StateStorage       bool
	HeightFilter       int8
	EvictionDepth      int8
	MetricsProxy       metrics.Proxy
}

func DefaultTreeOptions() TreeOptions {
	return TreeOptions{
		CheckpointInterval: 1000,
		StateStorage:       true,
		HeightFilter:       1,
		EvictionDepth:      -1,
		MetricsProxy:       &metrics.NilMetrics{},
	}
}

func NewTree(sql *SqliteDb, pool *NodePool, opts TreeOptions) *Tree {
	ctx, cancel := context.WithCancel(context.Background())
	tree := &Tree{
		sql:                sql,
		sqlWriter:          sql.newSQLWriter(),
		writerCancel:       cancel,
		pool:               pool,
		checkpoints:        &VersionRange{},
		metrics:            opts.MetricsProxy,
		maxWorkingSize:     1.5 * 1024 * 1024 * 1024,
		checkpointInterval: opts.CheckpointInterval,
		checkpointMemory:   opts.CheckpointMemory,
		storeLeafValues:    opts.StateStorage,
		storeLatestLeaves:  false,
		heightFilter:       opts.HeightFilter,
		metricsProxy:       opts.MetricsProxy,
		evictionDepth:      opts.EvictionDepth,
		leafSequence:       leafSequenceStart,
	}

	tree.sqlWriter.start(ctx)
	return tree
}

func (tree *Tree) LoadVersion(version int64) (err error) {
	if tree.sql == nil {
		return errors.New("sql is nil")
	}

	tree.workingBytes = 0
	tree.workingSize = 0

	tree.checkpoints, err = tree.sql.loadCheckpointRange()
	if err != nil {
		return err
	}
	tree.version = tree.checkpoints.FindPrevious(version)

	tree.root, err = tree.sql.LoadRoot(tree.version)
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

		if err = tree.replayChangelog(version, targetHash); err != nil {
			return err
		}
	}

	return nil
}

func (tree *Tree) LoadSnapshot(version int64, traverseOrder TraverseOrderType) (err error) {
	var v int64
	tree.root, v, err = tree.sql.ImportMostRecentSnapshot(version, traverseOrder, true)
	if err != nil {
		return err
	}
	if v < version {
		return fmt.Errorf("requested %d found snapshot %d, replay not yet supported", version, v)
	}
	tree.version = v
	tree.checkpoints, err = tree.sql.loadCheckpointRange()
	if err != nil {
		return err
	}
	return nil
}

func (tree *Tree) SaveSnapshot() (err error) {
	ctx := context.Background()
	return tree.sql.Snapshot(ctx, tree)
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.version++
	tree.resetSequences()

	if err := tree.sql.closeHangingIterators(); err != nil {
		return nil, 0, err
	}

	if !tree.shouldCheckpoint {
		tree.shouldCheckpoint = tree.version == 1 ||
			(tree.checkpointInterval > 0 && tree.version-tree.checkpoints.Last() >= tree.checkpointInterval) ||
			(tree.checkpointMemory > 0 && tree.workingBytes >= tree.checkpointMemory)
	}
	rootHash := tree.computeHash()

	err := tree.sqlWriter.saveTree(tree)
	if err != nil {
		return nil, tree.version, err
	}

	if tree.shouldCheckpoint {
		tree.branchOrphans = nil
		if err = tree.checkpoints.Add(tree.version); err != nil {
			return nil, tree.version, err
		}

		// if we've checkpointed without loading any tree node reads this means this was the first checkpoint.
		// shard queries will not be loaded. initialize them now.
		if tree.shouldCheckpoint && tree.sql.readConn == nil {
			if err := tree.sql.ResetShardQueries(); err != nil {
				return nil, tree.version, err
			}
		}
	}
	tree.leafOrphans = nil
	tree.leaves = nil
	tree.branches = nil
	tree.deletes = nil
	tree.shouldCheckpoint = false

	return rootHash, tree.version, nil
}

// ComputeHash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the tree root node hash.
// If the tree is empty (i.e. the node is nil), returns the hash of an empty input,
// to conform with RFC-6962.
func (tree *Tree) computeHash() []byte {
	if tree.root == nil {
		return sha256.New().Sum(nil)
	}
	tree.deepHash(tree.root, 0)
	return tree.root.hash
}

func (tree *Tree) deepHash(node *Node, depth int8) {
	if node == nil {
		panic(fmt.Sprintf("node is nil; sql.path=%s", tree.sql.opts.Path))
	}
	if node.isLeaf() {
		// new leaves are written every version
		if node.nodeKey.Version() == tree.version {
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
		tree.deepHash(node.left(tree), depth+1)
		tree.deepHash(node.right(tree), depth+1)
	}

	if !tree.shouldCheckpoint {
		// when not checkpointing, end recursion at a node with a hash (node.version < tree.version)
		if node.hash != nil {
			return
		}
	} else {
		// otherwise accumulate the branch node for checkpointing
		tree.branches = append(tree.branches, node)

		// if the node is missing a hash then it's children have already been loaded above.
		// if the node has a hash then traverse the dirty path.
		if node.hash != nil {
			if node.leftNode != nil {
				tree.deepHash(node.leftNode, depth+1)
			}
			if node.rightNode != nil {
				tree.deepHash(node.rightNode, depth+1)
			}
		}
	}

	node._hash()

	// when heightFilter > 0 remove the leaf nodes from memory.
	// if the leaf node is not dirty, return it to the pool.
	// if the leaf node is dirty, it will be written to storage then removed from the pool.
	if tree.heightFilter > 0 {
		if node.leftNode != nil && node.leftNode.isLeaf() {
			if !node.leftNode.dirty {
				tree.returnNode(node.leftNode)
			}
			node.leftNode = nil
		}
		if node.rightNode != nil && node.rightNode.isLeaf() {
			if !node.rightNode.dirty {
				tree.returnNode(node.rightNode)
			}
			node.rightNode = nil
		}
	}

	// finally, if checkpointing, remove node's children from memory if we're at the eviction height
	if tree.shouldCheckpoint {
		if depth >= tree.evictionDepth {
			node.evictChildren()
		}
	}
}

func (tree *Tree) Get(key []byte) ([]byte, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_get")
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
		_, res, err = tree.root.get(tree, key)
	}
	return res, err
}

func (tree *Tree) Has(key []byte) (bool, error) {
	if tree.metricsProxy != nil {
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_has")
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
		_, val, err = tree.root.get(tree, key)
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
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_set")
	}
	updated, err = tree.set(key, value)
	if err != nil {
		return false, err
	}
	if updated {
		tree.metrics.IncrCounter(1, metricsNamespace, "tree_update")
	} else {
		tree.metrics.IncrCounter(1, metricsNamespace, "tree_new_node")
	}
	return updated, nil
}

func (tree *Tree) set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.root == nil {
		tree.root = tree.NewLeafNode(key, value)
		return updated, nil
	}

	tree.root, updated, err = tree.recursiveSet(tree.root, key, value)
	return updated, err
}

func (tree *Tree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	if node == nil {
		panic("node is nil")
	}
	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1: // setKey < leafKey
			tree.metrics.IncrCounter(2, metricsNamespace, "pool_get")
			parent := tree.pool.Get()
			parent.nodeKey = tree.nextNodeKey()
			parent.key = node.key
			parent.subtreeHeight = 1
			parent.size = 2
			parent.dirty = true
			parent.setLeft(tree.NewLeafNode(key, value))
			parent.setRight(node)

			tree.workingBytes += parent.sizeBytes()
			tree.workingSize++
			return parent, false, nil
		case 1: // setKey > leafKey
			tree.metrics.IncrCounter(2, metricsNamespace, "pool_get")
			parent := tree.pool.Get()
			parent.nodeKey = tree.nextNodeKey()
			parent.key = key
			parent.subtreeHeight = 1
			parent.size = 2
			parent.dirty = true
			parent.setLeft(node)
			parent.setRight(tree.NewLeafNode(key, value))

			tree.workingBytes += parent.sizeBytes()
			tree.workingSize++
			return parent, false, nil
		default:
			tree.addOrphan(node)
			wasDirty := node.dirty
			tree.mutateNode(node)
			if tree.isReplaying {
				node.hash = value
			} else {
				if wasDirty {
					tree.workingBytes -= node.sizeBytes()
				}
				node.value = value
				node._hash()
				if !tree.storeLeafValues {
					node.value = nil
				}
				tree.workingBytes += node.sizeBytes()
			}
			return node, true, nil
		}

	} else {
		tree.addOrphan(node)
		tree.mutateNode(node)

		var child *Node
		if bytes.Compare(key, node.key) < 0 {
			child, updated, err = tree.recursiveSet(node.left(tree), key, value)
			if err != nil {
				return nil, updated, err
			}
			node.setLeft(child)
		} else {
			child, updated, err = tree.recursiveSet(node.right(tree), key, value)
			if err != nil {
				return nil, updated, err
			}
			node.setRight(child)
		}

		if updated {
			return node, updated, nil
		}
		err = node.calcHeightAndSize(tree)
		if err != nil {
			return nil, false, err
		}
		newNode, err := tree.balance(node)
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
		defer tree.metricsProxy.MeasureSince(time.Now(), metricsNamespace, "tree_remove")
	}

	if tree.root == nil {
		return nil, false, nil
	}
	newRoot, _, value, removed, err := tree.recursiveRemove(tree.root, key)
	if err != nil {
		return nil, false, err
	}
	if !removed {
		return nil, false, nil
	}

	tree.metrics.IncrCounter(1, metricsNamespace, "tree_delete")

	tree.root = newRoot
	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
func (tree *Tree) recursiveRemove(node *Node, key []byte) (newSelf *Node, newKey []byte, newValue []byte, removed bool, err error) {
	if node.isLeaf() {
		if bytes.Equal(key, node.key) {
			// we don't create an orphan here because the leaf node is removed
			tree.addDelete(node)
			tree.returnNode(node)
			return nil, nil, node.value, true, nil
		}
		return node, nil, nil, false, nil
	}

	// node.key < key; we go to the left to find the key:
	if bytes.Compare(key, node.key) < 0 {
		newLeftNode, newKey, value, removed, err := tree.recursiveRemove(node.left(tree), key)
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
			right := node.right(tree)
			k := node.key
			tree.returnNode(node)
			return right, k, value, removed, nil
		}

		tree.mutateNode(node)

		node.setLeft(newLeftNode)
		err = node.calcHeightAndSize(tree)
		if err != nil {
			return nil, nil, nil, false, err
		}
		node, err = tree.balance(node)
		if err != nil {
			return nil, nil, nil, false, err
		}

		return node, newKey, value, removed, nil
	}
	// node.key >= key; either found or look to the right:
	newRightNode, newKey, value, removed, err := tree.recursiveRemove(node.right(tree), key)
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
		left := node.left(tree)
		tree.returnNode(node)
		return left, nil, value, removed, nil
	}

	tree.mutateNode(node)

	node.setRight(newRightNode)
	if newKey != nil {
		node.key = newKey
	}
	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, nil, nil, false, err
	}

	node, err = tree.balance(node)
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
	nk := NewNodeKey(tree.version+1, tree.branchSequence)
	return nk
}

func (tree *Tree) nextLeafNodeKey() NodeKey {
	tree.leafSequence++
	if tree.leafSequence < leafSequenceStart {
		panic("leaf sequence underflow")
	}
	nk := NewNodeKey(tree.version+1, tree.leafSequence)
	return nk
}

func (tree *Tree) resetSequences() {
	tree.leafSequence = leafSequenceStart
	tree.branchSequence = 0
}

func (tree *Tree) mutateNode(node *Node) {
	// this second conditional is only relevant in replay; or more specifically, in cases where hashing has been
	// deferred between versions
	if node.hash == nil && node.nodeKey.Version() == tree.version+1 {
		return
	}
	node.hash = nil
	if node.isLeaf() {
		node.nodeKey = tree.nextLeafNodeKey()
	} else {
		node.nodeKey = tree.nextNodeKey()
	}

	if node.dirty {
		return
	}

	node.dirty = true
	tree.workingSize++
	if !node.isLeaf() {
		tree.workingBytes += node.sizeBytes()
	}
}

func (tree *Tree) addOrphan(node *Node) {
	if node.hash == nil {
		return
	}
	if !node.isLeaf() && node.nodeKey.Version() <= tree.checkpoints.Last() {
		tree.branchOrphans = append(tree.branchOrphans, node.nodeKey)
	} else if node.isLeaf() && !node.dirty {
		tree.leafOrphans = append(tree.leafOrphans, node.nodeKey)
	}
}

func (tree *Tree) addDelete(node *Node) {
	// added and removed in the same version; no op.
	if node.nodeKey.Version() == tree.version+1 {
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

	if tree.isReplaying {
		node.hash = value
	} else {
		node.value = value
		node._hash()
		if !tree.storeLeafValues {
			node.value = nil
		}
	}

	node.dirty = true
	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
	return node
}

func (tree *Tree) returnNode(node *Node) {
	if node.dirty {
		tree.workingBytes -= node.sizeBytes()
		tree.workingSize--
	}
	tree.pool.Put(node)
}

func (tree *Tree) Close() error {
	tree.writerCancel()
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

func (tree *Tree) replayChangelog(toVersion int64, targetHash []byte) error {
	return tree.sql.replayChangelog(tree, toVersion, targetHash)
}

func (tree *Tree) DeleteVersionsTo(toVersion int64) error {
	tree.sqlWriter.treePruneCh <- &pruneSignal{pruneVersion: toVersion, checkpoints: *tree.checkpoints}
	tree.sqlWriter.leafPruneCh <- &pruneSignal{pruneVersion: toVersion, checkpoints: *tree.checkpoints}
	return nil
}

func (tree *Tree) WorkingBytes() uint64 {
	return tree.workingBytes
}

func (tree *Tree) SetShouldCheckpoint() {
	tree.shouldCheckpoint = true
}
