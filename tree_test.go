// TODO move to package iavl_test
// this means an audit of exported fields and types.
package iavl

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func MemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	s := fmt.Sprintf("alloc=%s gc=%d",
		humanize.Bytes(m.Alloc),
		//humanize.Bytes(m.TotalAlloc),
		//humanize.Bytes(m.Sys),
		m.NumGC)
	return s
}

func testTreeBuild(t *testing.T, multiTree *MultiTree, opts *testutil.TreeBuildOptions) (cnt int64) {
	var (
		version                     int64
		err                         error
		lastCacheMiss, lastCacheHit int64
	)
	cnt = 1

	// generator
	itr := opts.Iterator
	fmt.Printf("Initial memory usage from generators:\n%s\n", MemUsage())

	sampleRate := int64(100_000)
	if opts.SampleRate != 0 {
		sampleRate = opts.SampleRate
	}

	since := time.Now()
	itrStart := time.Now()
	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		changeset := itr.Nodes()
		for ; changeset.Valid(); err = changeset.Next() {
			cnt++
			require.NoError(t, err)
			node := changeset.GetNode()

			//var keyBz bytes.Buffer
			//keyBz.Write([]byte(node.StoreKey))
			//keyBz.Write(node.Key)
			//key := keyBz.Bytes()
			key := node.Key

			tree, ok := multiTree.Trees[node.StoreKey]
			if !ok {
				require.NoError(t, multiTree.MountTree(node.StoreKey))
				tree = multiTree.Trees[node.StoreKey]
			}

			if !node.Delete {
				_, err = tree.Set(key, node.Value)
				require.NoError(t, err)
			} else {
				_, _, err := tree.Remove(key)
				require.NoError(t, err)
			}

			if cnt%sampleRate == 0 {
				dur := time.Since(since)

				var hitCount, missCount int64
				if tree.metrics.CacheHit < lastCacheHit {
					hitCount = tree.metrics.CacheHit
				} else {
					hitCount = tree.metrics.CacheHit - lastCacheHit
				}
				if tree.metrics.CacheMiss < lastCacheMiss {
					missCount = tree.metrics.CacheMiss
				} else {
					missCount = tree.metrics.CacheMiss - lastCacheMiss
				}
				lastCacheHit = tree.metrics.CacheHit
				lastCacheMiss = tree.metrics.CacheMiss

				fmt.Printf("leaves=%s time=%s last=%s μ=%s version=%d Δhit=%s Δmiss=%s %s\n",
					humanize.Comma(cnt),
					dur.Round(time.Millisecond),
					humanize.Comma(int64(float64(sampleRate)/time.Since(since).Seconds())),
					humanize.Comma(int64(float64(cnt)/time.Since(itrStart).Seconds())),
					version,
					humanize.Comma(hitCount),
					humanize.Comma(missCount),
					MemUsage())

				if tree.metrics.WriteTime > 0 {
					fmt.Printf("writes: cnt=%s wr/s=%s dur/wr=%s dur=%s\n",
						humanize.Comma(tree.metrics.WriteLeaves),
						humanize.Comma(int64(float64(tree.metrics.WriteLeaves)/tree.metrics.WriteTime.Seconds())),
						time.Duration(int64(tree.metrics.WriteTime)/tree.metrics.WriteLeaves),
						tree.metrics.WriteTime.Round(time.Millisecond),
					)
				}

				if err := tree.metrics.QueryReport(0); err != nil {
					t.Fatalf("query report err %v", err)
				}

				fmt.Println()

				since = time.Now()

				tree.metrics.WriteDurations = nil
				tree.metrics.WriteLeaves = 0
				tree.metrics.WriteTime = 0
			}
		}

		_, version, err = multiTree.SaveVersionConcurrently()
		require.NoError(t, err)

		require.NoError(t, err)
		if version == opts.Until {
			break
		}
	}
	fmt.Printf("final version: %d, hash: %x\n", version, multiTree.Hash())
	for sk, tree := range multiTree.Trees {
		fmt.Printf("storekey: %s height: %d, size: %d\n", sk, tree.Height(), tree.Size())
	}
	fmt.Printf("mean leaves/ms %s\n", humanize.Comma(cnt/time.Since(itrStart).Milliseconds()))
	multiTree.treeOpts.Metrics.Report()
	require.Equal(t, version, opts.Until)
	require.Equal(t, opts.UntilHash, fmt.Sprintf("%x", multiTree.Hash()))
	return cnt
}

func TestTree_Hash(t *testing.T) {
	var err error

	tmpDir := t.TempDir()
	t.Logf("levelDb tmpDir: %s\n", tmpDir)

	require.NoError(t, err)
	opts := testutil.BigTreeOptions_100_000()

	// this hash was validated as correct (with this same dataset) in iavl-bench
	// with `go run . tree --seed 1234 --dataset std`
	// at this commit tree: https://github.com/cosmos/iavl-bench/blob/3a6a1ec0a8cbec305e46239454113687da18240d/iavl-v0/main.go#L136
	opts.Until = 100
	opts.UntilHash = "0101e1d6f3158dcb7221acd7ed36ce19f2ef26847ffea7ce69232e362539e5cf"

	_, cancel := context.WithCancel(context.Background())

	testStart := time.Now()
	multiTree := NewMultiTree(tmpDir, TreeOptions{CheckpointInterval: 10})
	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
	require.True(t, ok)
	for _, sk := range itrs.StoreKeys() {
		require.NoError(t, multiTree.MountTree(sk))
	}
	leaves := testTreeBuild(t, multiTree, opts)
	treeDuration := time.Since(testStart)
	fmt.Printf("mean leaves/s: %s\n", humanize.Comma(int64(float64(leaves)/treeDuration.Seconds())))

	cancel()
	require.NoError(t, multiTree.Close())
}

func TestTree_Build_Load(t *testing.T) {
	// build the initial version of the tree with periodic checkpoints
	tmpDir := t.TempDir()
	opts := testutil.NewTreeBuildOptions().With10_000()
	multiTree := NewMultiTree(tmpDir, TreeOptions{CheckpointInterval: 4000})
	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
	require.True(t, ok)
	for _, sk := range itrs.StoreKeys() {
		require.NoError(t, multiTree.MountTree(sk))
	}
	testTreeBuild(t, multiTree, opts)

	// take a snapshot at version 10,000
	require.NoError(t, multiTree.SnapshotConcurrently())

	// import the snapshot into a new tree
	mt, err := ImportMultiTree(multiTree.pool, 10_000, tmpDir)
	require.NoError(t, err)

	// build the tree to version 12,000
	require.NoError(t, opts.Iterator.Next())
	require.Equal(t, int64(10_001), opts.Iterator.Version())
	opts.Until = 12_000
	opts.UntilHash = "3a037f8dd67a5e1a9ef83a53b81c619c9ac0233abee6f34a400fb9b9dfbb4f8d"
	testTreeBuild(t, mt, opts)

	// export the tree at version 12,000 and import it into a sql db
	ctx := context.Background()
	traverseOrder := PreOrder
	restoreMt := NewMultiTree(t.TempDir(), TreeOptions{CheckpointInterval: 4000})
	for sk, tree := range multiTree.Trees {
		require.NoError(t, restoreMt.MountTree(sk))
		exporter := tree.Export(traverseOrder)

		restoreTree := restoreMt.Trees[sk]
		_, err := restoreTree.sql.WriteSnapshot(ctx, tree.Version(), exporter.Next, SnapshotOptions{SaveTree: true, TraverseOrder: traverseOrder})
		require.NoError(t, err)
		require.NoError(t, restoreTree.LoadSnapshot(tree.Version()))
	}

	// play changes until version 20_000
	require.NoError(t, opts.Iterator.Next())
	require.Equal(t, int64(12_001), opts.Iterator.Version())
	opts.Until = 20_000
	opts.UntilHash = "25907b193c697903218d92fa70a87ef6cdd6fa5b9162d955a4d70a9d5d2c4824"
	testTreeBuild(t, restoreMt, opts)
}

func TestOsmoLike_HotStart(t *testing.T) {
	tmpDir := "/tmp/iavl-v2"
	pool := NewNodePool()
	multiTree, err := ImportMultiTree(pool, 1, tmpDir)
	require.NoError(t, err)
	require.NotNil(t, multiTree)
	opts := testutil.CompactedChangelogs("/tmp/osmo-like-many/v2")
	opts.SampleRate = 250_000
	testTreeBuild(t, multiTree, opts)
}

func TestOsmoLike_ColdStart(t *testing.T) {
	tmpDir := "/tmp/iavl-alpha6"

	multiTree := NewMultiTree(tmpDir, TreeOptions{CheckpointInterval: 100})
	require.NoError(t, multiTree.MountTrees())
	require.NoError(t, multiTree.LoadVersion(1))
	require.NoError(t, multiTree.WarmLeaves())

	opts := testutil.CompactedChangelogs("/Users/mattk/src/scratch/osmo-like-many/v2")
	opts.SampleRate = 250_000

	opts.Until = 1_000
	opts.UntilHash = "2020d5d28e2636c537e644fce53f057a706316ad8092a015bcaf2a7e153de468"

	testTreeBuild(t, multiTree, opts)
}

func TestTree_Import(t *testing.T) {
	tmpDir := "/Users/mattk/src/scratch/sqlite/height-zero"

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)

	root, err := sql.ImportSnapshotFromTable(1, true)
	require.NoError(t, err)
	require.NotNil(t, root)
}

func TestTree_Rehash(t *testing.T) {
	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: "/Users/mattk/src/scratch/sqlite/height-zero"})
	require.NoError(t, err)
	tree := NewTree(sql, pool, TreeOptions{})
	require.NoError(t, tree.LoadVersion(1))

	savedHash := make([]byte, 32)
	n := copy(savedHash, tree.root.hash)
	require.Equal(t, 32, n)
	var step func(node *Node)
	step = func(node *Node) {
		if node.isLeaf() {
			return
		}
		node.hash = nil
		step(node.left(tree))
		step(node.right(tree))
		node._hash(1)
	}
	step(tree.root)
	require.Equal(t, savedHash, tree.root.hash)
}

func TestTreeSanity(t *testing.T) {
	cases := []struct {
		name   string
		treeFn func() *Tree
		hashFn func(*Tree) []byte
	}{
		{
			name: "sqlite",
			treeFn: func() *Tree {
				pool := NewNodePool()
				sql, err := NewInMemorySqliteDb(pool)
				require.NoError(t, err)
				return NewTree(sql, pool, TreeOptions{})
			},
			hashFn: func(tree *Tree) []byte {
				hash, _, err := tree.SaveVersion()
				require.NoError(t, err)
				return hash
			},
		},
		{
			name: "no db",
			treeFn: func() *Tree {
				pool := NewNodePool()
				return NewTree(nil, pool, TreeOptions{})
			},
			hashFn: func(tree *Tree) []byte {
				rehashTree(tree.root)
				tree.version++
				return tree.root.hash
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := tc.treeFn()
			opts := testutil.NewTreeBuildOptions()
			itr := opts.Iterator
			var err error
			for ; itr.Valid(); err = itr.Next() {
				if itr.Version() > 150 {
					break
				}
				require.NoError(t, err)
				nodes := itr.Nodes()
				for ; nodes.Valid(); err = nodes.Next() {
					require.NoError(t, err)
					node := nodes.GetNode()
					if node.Delete {
						_, _, err := tree.Remove(node.Key)
						require.NoError(t, err)
					} else {
						_, err := tree.Set(node.Key, node.Value)
						require.NoError(t, err)
					}
				}
				switch itr.Version() {
				case 1:
					h := tc.hashFn(tree)
					require.Equal(t, "48c3113b8ba523d3d539d8aea6fce28814e5688340ba7334935c1248b6c11c7a", hex.EncodeToString(h))
					require.Equal(t, int64(104938), tree.root.size)
					fmt.Printf("version=%d, hash=%x size=%d\n", itr.Version(), h, tree.root.size)
				case 150:
					h := tc.hashFn(tree)
					require.Equal(t, "04c42dd1cec683cbbd4974027e4b003b848e389a33d03d7a9105183e6d108dd9", hex.EncodeToString(h))
					require.Equal(t, int64(105030), tree.root.size)
					fmt.Printf("version=%d, hash=%x size=%d\n", itr.Version(), h, tree.root.size)
				}
			}
		})
	}
}
