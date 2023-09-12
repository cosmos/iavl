// TODO move to package iavl_test
// this means an audit of exported fields and types.
package iavl

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cosmos/iavl/v2/leveldb"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func testTreeBuild(t *testing.T, tree *Tree, opts testutil.TreeBuildOptions) {
	var (
		hash    []byte
		version int64
		cnt     int64 = 1
		since         = time.Now()
		err     error
	)

	// log file
	//itr, err := compact.NewChangesetIterator("/Users/mattk/src/scratch/osmosis-hist/ordered/bank", "bank")
	//require.NoError(t, err)
	//opts.Until = math.MaxInt64

	// generator
	itr := opts.Iterator

	itrStart := time.Now()
	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		for _, node := range itr.GetChangeset().Nodes {
			var keyBz bytes.Buffer
			keyBz.Write([]byte(node.StoreKey))
			keyBz.Write(node.Key)
			key := keyBz.Bytes()

			if !node.Delete {
				_, err = tree.Set(key, node.Value)
				require.NoError(t, err)
			} else {
				_, _, err := tree.Remove(key)
				require.NoError(t, err)
			}

			if cnt%100_000 == 0 {
				fmt.Printf("processed %s leaves in %s; %s leaves/s; version=%d\n",
					humanize.Comma(int64(cnt)),
					time.Since(since),
					humanize.Comma(int64(100_000/time.Since(since).Seconds())),
					version)
				since = time.Now()
			}
			cnt++
		}
		hash, version, err = tree.SaveVersion()
		require.NoError(t, err)
		if version == opts.Until {
			break
		}
	}
	fmt.Printf("final version: %d, hash: %x\n", version, hash)
	fmt.Printf("height: %d, size: %d\n", tree.Height(), tree.Size())
	fmt.Printf("mean leaves/ms %s\n", humanize.Comma(cnt/time.Since(itrStart).Milliseconds()))
	if opts.Report != nil {
		opts.Report()
	}
	require.Equal(t, opts.UntilHash, fmt.Sprintf("%x", hash))
	require.Equal(t, version, opts.Until)
}

func TestTree_Build(t *testing.T) {
	//just a little bigger than the size of the initial changeset. evictions will occur slowly.
	poolSize := 210_050
	// no evictions
	//poolSize := 500_000
	// overflow on initial changeset and frequently after; worst performance
	//poolSize := 100_000

	var err error
	//db := newMapDB()

	tmpDir := t.TempDir()
	t.Logf("levelDb tmpDir: %s\n", tmpDir)
	levelDb, err := leveldb.New("iavl_test", tmpDir)
	require.NoError(t, err)
	db := &kvDB{db: levelDb}

	tree := &Tree{
		pool:               newNodePool(db, poolSize),
		metrics:            &metrics.TreeMetrics{},
		db:                 db,
		checkpointInterval: 10_000,
	}
	tree.pool.metrics = tree.metrics

	opts := testutil.NewTreeBuildOptions()
	opts.Report = func() {
		tree.metrics.Report()
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		checkpointErr := tree.pool.CheckpointRunner(ctx)
		require.NoError(t, checkpointErr)
	}()

	testTreeBuild(t, tree, opts)

	err = tree.Checkpoint()
	require.NoError(t, err)
	// wait
	tree.pool.checkpointCh <- &checkpointArgs{version: -1}

	// don't evict root on iteration, it interacts with the node pool
	tree.root.dirty = true
	count := pooledTreeCount(tree, *tree.root)
	height := pooledTreeHeight(tree, *tree.root)

	workingSetCount := -1 // offset the dirty root above.
	for _, n := range tree.pool.nodes {
		if n.dirty {
			workingSetCount++
		}
	}

	fmt.Printf("workingSetCount: %d\n", workingSetCount)
	fmt.Printf("treeCount: %d\n", count)
	fmt.Printf("treeHeight: %d\n", height)

	// TODO
	// equivalence between dbs

	//fmt.Printf("db stats:\n sets: %s, deletes: %s\n",
	//	humanize.Comma(int64(db.setCount)),
	//	humanize.Comma(int64(db.deleteCount)))

	require.Equal(t, height, tree.root.SubtreeHeight+1)
	//require.Equal(t, count, len(db.nodes))

	require.Equal(t, tree.pool.dirtyCount, workingSetCount)

	treeAndDbEqual(t, tree, *tree.root)
}

func treeCount(node *Node) int {
	if node == nil {
		return 0
	}
	return 1 + treeCount(node.leftNode) + treeCount(node.rightNode)
}

func pooledTreeCount(tree *Tree, node Node) int {
	if node.isLeaf() {
		return 1
	}
	left := *node.left(tree)
	right := *node.right(tree)
	return 1 + pooledTreeCount(tree, left) + pooledTreeCount(tree, right)
}

func pooledTreeHeight(tree *Tree, node Node) int8 {
	if node.isLeaf() {
		return 1
	}
	left := *node.left(tree)
	right := *node.right(tree)
	return 1 + maxInt8(pooledTreeHeight(tree, left), pooledTreeHeight(tree, right))
}

func treeAndDbEqual(t *testing.T, tree *Tree, node Node) {
	dbNode, err := tree.db.Get(*node.NodeKey)
	if err != nil {
		t.Fatalf("error getting node from db: %s", err)
	}
	require.NoError(t, err)
	require.NotNil(t, dbNode)
	require.Equal(t, dbNode.NodeKey, node.NodeKey)
	require.Equal(t, dbNode.Key, node.Key)
	require.Equal(t, dbNode.Value, node.Value)
	require.Equal(t, dbNode.Size, node.Size)
	require.Equal(t, dbNode.SubtreeHeight, node.SubtreeHeight)
	if node.isLeaf() {
		return
	}
	require.Equal(t, dbNode.LeftNodeKey, node.LeftNodeKey)
	require.Equal(t, dbNode.RightNodeKey, node.RightNodeKey)

	leftNode := *node.left(tree)
	rightNode := *node.right(tree)
	treeAndDbEqual(t, tree, leftNode)
	treeAndDbEqual(t, tree, rightNode)
}
