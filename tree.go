package iavl

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

var log = zlog.Output(zerolog.ConsoleWriter{
	Out:        os.Stderr,
	TimeFormat: time.Stamp,
})

type nodeDelete struct {
	// the node key to delete
	nodeKey NodeKey
	// the sequence in which this deletion was processed
	deleteKey NodeKey
}

type Tree struct {
	version int64
	root    *Node
	metrics *metrics.TreeMetrics
	sql     *SqliteDb
	cache   *NodeCache
	pool    *NodePool

	lastCheckpoint     int64
	checkpointInterval int64
	shouldCheckpoint   bool

	workingBytes uint64
	workingSize  int64

	// options
	maxWorkingSize uint64

	branches []*Node
	leaves   []*Node
	orphans  []NodeKey
	deletes  []*nodeDelete
	sequence uint32
}

type TreeOptions struct {
	CheckpointInterval int64
	Metrics            *metrics.TreeMetrics
}

func NewTree(sql *SqliteDb, pool *NodePool, opts TreeOptions) *Tree {
	if opts.Metrics == nil {
		opts.Metrics = &metrics.TreeMetrics{}
	}
	tree := &Tree{
		sql:                sql,
		pool:               pool,
		cache:              NewNodeCache(opts.Metrics),
		metrics:            opts.Metrics,
		maxWorkingSize:     2 * 1024 * 1024 * 1024,
		checkpointInterval: opts.CheckpointInterval,
	}
	return tree
}

func (tree *Tree) LoadVersion(version int64) error {
	if tree.sql == nil {
		return fmt.Errorf("sql is nil")
	}

	tree.version = version
	tree.root = nil
	tree.orphans = nil
	tree.workingBytes = 0
	tree.workingSize = 0
	tree.cache.Swap()

	var err error
	tree.root, err = tree.sql.LoadRoot(version)
	if err != nil {
		return err
	}
	// TODO
	tree.lastCheckpoint = version
	return nil
}

func (tree *Tree) WarmTree() error {
	var i int
	start := time.Now()
	log.Info().Msgf("loading tree into memory version=%d", tree.version)
	loadTreeSince := time.Now()
	tree.loadTree(&i, &loadTreeSince, tree.root)
	log.Info().Msgf("loaded %s tree nodes into memory version=%d dur=%s",
		humanize.Comma(int64(i)),
		tree.version, time.Since(start).Round(time.Millisecond))
	err := tree.sql.WarmLeaves()
	if err != nil {
		return err
	}
	return tree.metrics.QueryReport(5)
}

func (tree *Tree) loadTree(i *int, since *time.Time, node *Node) *Node {
	if node.isLeaf() {
		return nil
	}
	*i++
	if *i%1_000_000 == 0 {
		log.Info().Msgf("loadTree i=%s, r/s=%s",
			humanize.Comma(int64(*i)),
			humanize.Comma(int64(1_000_000/time.Since(*since).Seconds())),
		)
		*since = time.Now()
	}
	// balanced subtree with two leaves, skip 2 queries
	if node.subtreeHeight == 1 || (node.subtreeHeight == 2 && node.size == 3) {
		return node
	}

	node.leftNode = tree.loadTree(i, since, node.left(tree))
	node.rightNode = tree.loadTree(i, since, node.right(tree))
	return node
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.version++
	tree.sequence = 0

	tree.shouldCheckpoint = tree.version == 1 ||
		(tree.checkpointInterval > 0 && tree.version-tree.lastCheckpoint >= tree.checkpointInterval)
	tree.deepHash(tree.root)
	needsCheckpoint := tree.shouldCheckpoint && len(tree.branches) > 0
	if tree.shouldCheckpoint {
		tree.lastCheckpoint = tree.version
	}

	writeStart := time.Now()
	if needsCheckpoint {
		log.Info().Msgf("checkpointing version=%d path=%s", tree.version, tree.sql.opts.Path)
		if err := tree.sql.NextShard(); err != nil {
			return nil, 0, err
		}
		tree.lastCheckpoint = tree.version
	}

	batch := newSqliteBatch(tree)
	_, versions, err := batch.save()
	if err != nil {
		return nil, tree.version, err
	}
	dur := time.Since(writeStart)
	tree.metrics.WriteDurations = append(tree.metrics.WriteDurations, dur)
	tree.metrics.WriteTime += dur
	tree.metrics.WriteLeaves += int64(len(tree.leaves))

	if tree.shouldCheckpoint && len(tree.branches) > 0 {
		err = tree.sql.MapVersions(versions, tree.sql.shardId)
		if err != nil {
			return nil, 0, err
		}

		err = tree.sql.addShardQuery()
		if err != nil {
			return nil, 0, err
		}

		//log.Info().Msg("creating leaf index")
		err = tree.sql.leafWrite.Exec("CREATE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence)")
		if err != nil {
			return nil, tree.version, err
		}
		//log.Info().Msg("creating leaf index done")
	}

	tree.leaves = nil
	tree.branches = nil
	tree.orphans = nil
	tree.deletes = nil
	tree.shouldCheckpoint = false

	err = tree.sql.SaveRoot(tree.version, tree.root)
	if err != nil {
		return nil, tree.version, fmt.Errorf("failed to save root: %w", err)
	}

	return tree.root.hash, tree.version, nil
}

func (tree *Tree) deepHash(node *Node) (isLeaf bool, isDirty bool) {
	isLeaf = node.isLeaf()

	// new leaves are written every version
	if node.nodeKey.Version() == tree.version {
		isDirty = true
		if isLeaf {
			tree.leaves = append(tree.leaves, node)
		}
	}

	// always end recursion at a leaf
	if isLeaf {
		return true, isDirty
	}

	if !tree.shouldCheckpoint {
		// when not checkpointing, end recursion at a node with a hash (node.version < tree.version)
		if node.hash != nil {
			return false, isDirty
		}
	} else {
		// when checkpointing end recursion at path where node.version <= tree.lastCheckpoint
		// and accumulate tree node into tree.branches
		if node.nodeKey.Version() <= tree.lastCheckpoint {
			return isLeaf, isDirty
		}
		tree.branches = append(tree.branches, node)
	}

	// When reading leaves, this will initiate a leafRead from storage for the sole purpose of producing a hash.
	// Recall that a terminal tree node may have only updated one leaf this version.
	// We can explore storing right/left hash in terminal tree nodes to avoid this, or changing the storage
	// format to iavl v0 where left/right hash are stored in the node.
	leftIsLeaf, leftisDirty := tree.deepHash(node.left(tree))
	rightIsLeaf, rightIsDirty := tree.deepHash(node.right(tree))

	node._hash(tree.version)

	// will be returned to the pool in BatchSet if not below
	if leftIsLeaf {
		if !leftisDirty {
			tree.pool.Put(node.leftNode)
		}
		node.leftNode = nil
	}
	if rightIsLeaf {
		if !rightIsDirty {
			tree.pool.Put(node.rightNode)
		}
		node.rightNode = nil
	}

	return false, isDirty
}

func (tree *Tree) dirtyCount(node *Node) int64 {
	var n int64
	if node.dirty {
		n = 1
	}

	if !node.isLeaf() {
		return n + tree.dirtyCount(node.left(tree)) + tree.dirtyCount(node.right(tree))
	} else {
		return n
	}
}

// Set sets a key in the working tree. Nil values are invalid. The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *Tree) Set(key, value []byte) (updated bool, err error) {
	updated, err = tree.set(key, value)
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

func (tree *Tree) set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.root == nil {
		tree.root = tree.NewNode(key, value, tree.version)
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
			tree.metrics.PoolGet += 2
			parent := tree.pool.Get()
			parent.nodeKey = tree.nextNodeKey()
			parent.key = node.key
			parent.subtreeHeight = 1
			parent.size = 2
			parent.dirty = true
			parent.setLeft(tree.NewNode(key, value, tree.version))
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
			parent.dirty = true
			parent.setLeft(node)
			parent.setRight(tree.NewNode(key, value, tree.version))

			tree.workingBytes += parent.sizeBytes()
			tree.workingSize++
			return parent, false, nil
		default:
			tree.addOrphan(node)
			tree.mutateNode(node)
			node.value = value
			node._hash(tree.version + 1)
			node.value = nil
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

	tree.metrics.TreeDelete++

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
			tree.addOrphan(node)
			tree.addDelete(node)
			tree.returnNode(node)
			return nil, nil, node.value, true, nil
		}
		return node, nil, nil, false, nil
	}

	if err != nil {
		return nil, nil, nil, false, err
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
	tree.sequence++
	nk := NewNodeKey(tree.version+1, tree.sequence)
	return nk
}

func (tree *Tree) mutateNode(node *Node) {
	// node has already been mutated in working set
	if node.hash == nil {
		return
	}
	node.hash = nil
	node.nodeKey = tree.nextNodeKey()

	if node.dirty {
		return
	}

	node.dirty = true
	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
}

func (tree *Tree) addOrphan(node *Node) {
	if node.hash == nil {
		return
	}
	if node.nodeKey.Version() > tree.lastCheckpoint {
		return
	}
	tree.orphans = append(tree.orphans, node.nodeKey)
}

func (tree *Tree) addDelete(node *Node) {
	// added and removed in the same version; no op.
	if node.nodeKey.Version() == tree.version+1 {
		return
	}
	tree.deletes = append(tree.deletes, &nodeDelete{nodeKey: node.nodeKey, deleteKey: tree.nextNodeKey()})
}

// NewNode returns a new node from a key, value and version.
func (tree *Tree) NewNode(key []byte, value []byte, version int64) *Node {
	node := tree.pool.Get()

	node.nodeKey = tree.nextNodeKey()

	node.key = key
	node.value = value
	node.subtreeHeight = 0
	node.size = 1

	node._hash(version + 1)
	node.value = nil
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
	return tree.sql.Close()
}
