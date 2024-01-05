package iavl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
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
	// the sequence in which this deletion was processed
	deleteKey NodeKey
	// the leaf key to delete in `latest` table (if maintained)
	leafKey []byte
}

type Tree struct {
	version      int64
	root         *Node
	metrics      *metrics.TreeMetrics
	sql          *SqliteDb
	sqlWriter    *sqlWriter
	writerCancel context.CancelFunc
	cache        *NodeCache
	pool         *NodePool

	lastCheckpoint   int64
	shouldCheckpoint bool

	// options
	maxWorkingSize     uint64
	workingBytes       uint64
	checkpointInterval int64
	workingSize        int64
	storeLeafValues    bool
	storeLatestLeaves  bool
	heightFilter       int8
	metricsProxy       metrics.Proxy

	// state
	branches      []*Node
	leaves        []*Node
	branchOrphans []NodeKey
	leafOrphans   []NodeKey
	deletes       []*nodeDelete
	sequence      uint32
	isReplaying   bool
}

type TreeOptions struct {
	CheckpointInterval int64
	Metrics            *metrics.TreeMetrics
	StateStorage       bool
	HeightFilter       int8
	MetricsProxy       metrics.Proxy
}

func DefaultTreeOptions() TreeOptions {
	return TreeOptions{
		CheckpointInterval: 1000,
		Metrics:            &metrics.TreeMetrics{},
		StateStorage:       true,
		HeightFilter:       1,
	}
}

func NewTree(sql *SqliteDb, pool *NodePool, opts TreeOptions) *Tree {
	if opts.Metrics == nil {
		opts.Metrics = &metrics.TreeMetrics{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	tree := &Tree{
		sql:                sql,
		sqlWriter:          sql.newSqlWriter(),
		writerCancel:       cancel,
		pool:               pool,
		cache:              NewNodeCache(opts.Metrics),
		metrics:            opts.Metrics,
		maxWorkingSize:     2 * 1024 * 1024 * 1024,
		checkpointInterval: opts.CheckpointInterval,
		storeLeafValues:    opts.StateStorage,
		storeLatestLeaves:  false,
		heightFilter:       opts.HeightFilter,
		metricsProxy:       opts.MetricsProxy,
	}

	tree.sqlWriter.start(ctx)
	return tree
}

func (tree *Tree) LoadVersion(version int64) (err error) {
	if tree.sql == nil {
		return fmt.Errorf("sql is nil")
	}

	tree.workingBytes = 0
	tree.workingSize = 0

	tree.lastCheckpoint, err = tree.sql.lastCheckpoint(version)
	if err != nil {
		return err
	}
	tree.version = tree.lastCheckpoint

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

		if err = tree.replayChangelog(version); err != nil {
			return err
		}
		rootHash := tree.computeHash()
		if !bytes.Equal(targetHash, rootHash) {
			return fmt.Errorf("root hash mismatch; expected %x got %x", targetRoot.hash, tree.root.hash)
		}
		tree.sql.logger.Info().Msgf("replayed to root: %v", tree.root)
		tree.version = version
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
	tree.lastCheckpoint = v
	return nil
}

func (tree *Tree) SaveSnapshot() (err error) {
	ctx := context.Background()
	return tree.sql.Snapshot(ctx, tree)
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

	if err := tree.sql.closeHangingIterators(); err != nil {
		return nil, 0, err
	}

	tree.shouldCheckpoint = tree.version == 1 ||
		(tree.checkpointInterval > 0 && tree.version-tree.lastCheckpoint >= tree.checkpointInterval)
	rootHash := tree.computeHash()
	// todo
	// move all checkpoint crap into sqlWriter

	writeStart := time.Now()

	err := tree.sqlWriter.saveTree(tree)
	if err != nil {
		return nil, tree.version, err
	}
	dur := time.Since(writeStart)
	tree.metrics.WriteDurations = append(tree.metrics.WriteDurations, dur)
	tree.metrics.WriteTime += dur
	tree.metrics.WriteLeaves += int64(len(tree.leaves))

	if tree.shouldCheckpoint {
		tree.branchOrphans = nil
		tree.lastCheckpoint = tree.version
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
	tree.deepHash(tree.root)
	return tree.root.hash
}

func (tree *Tree) deepHash(node *Node) (isLeaf bool, isDirty bool) {
	if node == nil {
		panic(fmt.Sprintf("node is nil; sql.path=%s", tree.sql.opts.Path))
	}
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

	node._hash()

	if tree.heightFilter > 0 {
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
	}

	return false, isDirty
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
		_, res, err = tree.root.get(tree, key)
	}
	return res, err
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
		defer tree.metricsProxy.MeasureSince(time.Now(), "iavl_v2", "set")
	}
	//tree.sql.logger.Debug().Msgf("set key=%x value=%x", key, value)
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
		tree.root = tree.NewNode(key, value)
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
			parent.setLeft(tree.NewNode(key, value))
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
			parent.setRight(tree.NewNode(key, value))

			tree.workingBytes += parent.sizeBytes()
			tree.workingSize++
			return parent, false, nil
		default:
			tree.addOrphan(node)
			tree.mutateNode(node)
			if tree.isReplaying {
				node.hash = value
			} else {
				node.value = value
				node._hash()
				if !tree.storeLeafValues {
					node.value = nil
				}
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
		tree.metricsProxy.MeasureSince(time.Now(), "iavL_v2", "remove")
	}
	//tree.sql.logger.Debug().Msgf("delete key=%x", key)

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
			// we don't create an orphan here because the leaf node is removed
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
	// this second conditional is only relevant in replay; or more specifically, in cases where hashing has been
	// deferred between versions
	if node.hash == nil && node.nodeKey.Version() == tree.version+1 {
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
	if node.isLeaf() {
		tree.leafOrphans = append(tree.leafOrphans, node.nodeKey)
	} else {
		tree.branchOrphans = append(tree.branchOrphans, node.nodeKey)
	}
}

func (tree *Tree) addDelete(node *Node) {
	// added and removed in the same version; no op.
	if node.nodeKey.Version() == tree.version+1 {
		return
	}
	del := &nodeDelete{
		deleteKey: tree.nextNodeKey(),
		leafKey:   node.key,
	}
	tree.deletes = append(tree.deletes, del)
}

// NewNode returns a new node from a key, value and version.
func (tree *Tree) NewNode(key []byte, value []byte) *Node {
	node := tree.pool.Get()

	node.nodeKey = tree.nextNodeKey()

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

func (tree *Tree) GetSql() *SqliteDb {
	return tree.sql
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
	var (
		since        = time.Now()
		batchSize    = 200_000
		count        = 0
		step         func(node *Node) error
		lg           = log.With().Str("path", tree.sql.opts.Path).Logger()
		latestInsert *sqlite3.Stmt
	)
	prepare := func() error {
		latestInsert, err = tree.sql.leafWrite.Prepare("INSERT INTO latest (key, value) VALUES (?, ?)")
		if err != nil {
			return err
		}
		if err = tree.sql.leafWrite.Begin(); err != nil {
			return err
		}
		return nil
	}

	flush := func() error {
		if err = tree.sql.leafWrite.Commit(); err != nil {
			return err
		}
		if err = latestInsert.Close(); err != nil {
			return err
		}
		var rate string
		if time.Since(since).Seconds() > 0 {
			rate = humanize.Comma(int64(float64(batchSize) / time.Since(since).Seconds()))
		} else {
			rate = "n/a"
		}
		lg.Info().Msgf("latest flush; count=%s dur=%s wr/s=%s",
			humanize.Comma(int64(count)),
			time.Since(since).Round(time.Millisecond),
			rate,
		)
		since = time.Now()
		return nil
	}

	maybeFlush := func() error {
		count++
		if count%batchSize == 0 {
			err = flush()
			if err != nil {
				return err
			}
			return prepare()
		}
		return nil
	}

	if err = prepare(); err != nil {
		return err
	}

	step = func(node *Node) error {
		if node.isLeaf() {
			err := latestInsert.Exec(node.key, node.value)
			if err != nil {
				return err
			}
			return maybeFlush()
		}
		if err = step(node.left(tree)); err != nil {
			return err
		}
		if err = step(node.right(tree)); err != nil {
			return err
		}
		return nil
	}

	err = step(tree.root)
	if err != nil {
		return err
	}
	err = flush()
	if err != nil {
		return err
	}

	return latestInsert.Close()
}

func (tree *Tree) replayChangelog(toVersion int64) error {
	var (
		version     int
		lastVersion int
		sequence    int
		bz          []byte
		key         []byte
		count       int64
		start       = time.Now()
		lg          = log.With().Str("path", tree.sql.opts.Path).Logger()
		since       = time.Now()
	)
	tree.isReplaying = true
	defer func() {
		tree.isReplaying = false
	}()

	lg.Info().Msgf("ensure leaf_delete_index exists...")
	if err := tree.sql.leafWrite.Exec("CREATE INDEX IF NOT EXISTS leaf_delete_idx ON leaf (version, sequence)"); err != nil {
		return err
	}
	lg.Info().Msg("...done")
	lg.Info().Msgf("replaying changelog from=%d to=%d", tree.version, toVersion)
	conn, err := tree.sql.getReadConn()
	if err != nil {
		return err
	}
	q, err := conn.Prepare(`SELECT * FROM (
		SELECT version, sequence, bytes, null AS key
	FROM leaf WHERE version > ? AND version <= ?
	UNION
	SELECT version, sequence, null as bytes, key
	FROM leaf_delete WHERE version > ? AND version <= ?
	) as ops
	ORDER BY version, sequence`)
	if err != nil {
		return err
	}
	if err = q.Bind(tree.version, toVersion, tree.version, toVersion); err != nil {
		return err
	}
	var ()
	for {
		ok, err := q.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		count++
		if err = q.Scan(&version, &sequence, &bz, &key); err != nil {
			return err
		}
		if version-1 != lastVersion {
			tree.leaves, tree.branches, tree.leafOrphans, tree.branchOrphans, tree.deletes = nil, nil, nil, nil, nil
			tree.version = int64(version - 1)
			tree.sequence = 0
			lastVersion = version - 1
		}
		if bz != nil {
			nk := NewNodeKey(0, 0)
			node, err := MakeNode(tree.pool, nk, bz)
			if err != nil {
				return err
			}
			if _, err = tree.Set(node.key, node.hash); err != nil {
				return err
			}
			//if sequence != int(tree.sequence) {
			//	return fmt.Errorf("sequence mismatch version=%d; expected %d got %d",
			//		version, sequence, tree.sequence)
			//}
		} else {
			//if sequence != int(tree.sequence+1) {
			//	return fmt.Errorf("sequence mismatch; version=%d expected %d got %d",
			//		version, sequence, tree.sequence+1)
			//}
			if _, _, err = tree.Remove(key); err != nil {
				return err
			}
		}
		if count%250_000 == 0 {
			lg.Info().Msgf("replayed changelog to version=%d count=%s node/s=%s",
				version, humanize.Comma(count), humanize.Comma(int64(250_000/time.Since(since).Seconds())))
			since = time.Now()
		}
	}
	tree.leaves, tree.branches, tree.leafOrphans, tree.branchOrphans, tree.deletes = nil, nil, nil, nil, nil
	tree.sequence = 0
	lg.Info().Msgf("replayed changelog to version=%d count=%s dur=%s",
		lastVersion, humanize.Comma(count), time.Since(start).Round(time.Millisecond))
	return q.Close()
}

func (tree *Tree) DeleteVersionsTo(toVersion int64) error {
	return tree.sql.DeleteVersionsTo(toVersion)
}
