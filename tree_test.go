// TODO move to package iavl_test
// this means an audit of exported fields and types.
package iavl

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
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

func testTreeBuild(t *testing.T, sqlOpts SqliteDbOptions, opts testutil.TreeBuildOptions) (cnt int64) {
	var (
		hash                        []byte
		version                     int64
		err                         error
		lastCacheMiss, lastCacheHit int64
	)
	cnt = 1

	multiTree := NewStdMultiTree(sqlOpts, NewNodePool())

	// generator
	itr := opts.Iterator
	fmt.Printf("Initial memory usage from generators:\n%s\n", MemUsage())

	// not supported in multi tree yet, may revive.
	//
	//if opts.LoadVersion != 0 {
	//	for _, tree := range multiTree.Trees {
	//		require.NoError(t, tree.LoadVersion(opts.LoadVersion))
	//	}
	//
	//	log.Info().Msgf("fast forwarding changesets to version %d...", opts.LoadVersion+1)
	//	i := 1
	//	for ; itr.Valid(); err = itr.Next() {
	//		if itr.Version() > opts.LoadVersion {
	//			break
	//		}
	//		require.NoError(t, err)
	//		nodes := itr.Nodes()
	//		for ; nodes.Valid(); err = nodes.Next() {
	//			require.NoError(t, err)
	//			if i%5_000_000 == 0 {
	//				fmt.Printf("fast forward %s nodes\n", humanize.Comma(int64(i)))
	//			}
	//			i++
	//		}
	//	}
	//	log.Info().Msgf("fast forward complete")
	//}

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
				require.NoError(t, multiTree.AddTree(node.StoreKey))
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
				if tree.cache.hitCount < lastCacheHit {
					hitCount = tree.cache.hitCount
				} else {
					hitCount = tree.cache.hitCount - lastCacheHit
				}
				if tree.cache.missCount < lastCacheMiss {
					missCount = tree.cache.missCount
				} else {
					missCount = tree.cache.missCount - lastCacheMiss
				}
				lastCacheHit = tree.cache.hitCount
				lastCacheMiss = tree.cache.missCount

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

				if tree.kv == nil {
					if err := tree.sql.queryReport(0); err != nil {
						t.Fatalf("query report err %v", err)
					}
				} else {
					if err = tree.kv.readReport(); err != nil {
						t.Fatalf("leafRead report err %v", err)
					}
				}

				fmt.Println()

				since = time.Now()

				tree.metrics.WriteDurations = nil
				tree.metrics.WriteLeaves = 0
				tree.metrics.WriteTime = 0
			}
		}

		_, version, err = multiTree.SaveVersion()
		require.NoError(t, err)

		require.NoError(t, err)
		if version == opts.Until {
			break
		}
	}
	fmt.Printf("final version: %d, hash: %x\n", version, hash)
	for sk, tree := range multiTree.Trees {
		fmt.Printf("storekey: %s height: %d, size: %d\n", sk, tree.Height(), tree.Size())
	}
	fmt.Printf("mean leaves/ms %s\n", humanize.Comma(cnt/time.Since(itrStart).Milliseconds()))
	multiTree.metrics.Report()
	require.Equal(t, version, opts.Until)
	require.Equal(t, opts.UntilHash, fmt.Sprintf("%x", multiTree.Hash()))
	return cnt
}

func TestTree_Build(t *testing.T) {
	var err error

	tmpDir := t.TempDir()
	t.Logf("levelDb tmpDir: %s\n", tmpDir)

	sqlOpts := SqliteDbOptions{Path: tmpDir}
	require.NoError(t, err)

	opts := testutil.OsmoLikeManyTrees()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	testStart := time.Now()
	leaves := testTreeBuild(t, sqlOpts, opts)

	treeDuration := time.Since(testStart)

	fmt.Printf("mean leaves/s: %s\n", humanize.Comma(int64(float64(leaves)/treeDuration.Seconds())))

	ts := &treeStat{}

	fmt.Printf("tree size: %s\n", humanize.Bytes(ts.size))

	cancel()
}

func treeCount(tree *Tree, node Node) int {
	if node.isLeaf() {
		return 1
	}
	left := node.left(tree)
	right := node.right(tree)
	return 1 + treeCount(tree, *left) + treeCount(tree, *right)
}

func treeHeight(tree *Tree, node Node) int8 {
	if node.isLeaf() {
		return 1
	}
	left := node.left(tree)
	right := node.right(tree)
	return 1 + maxInt8(treeHeight(tree, *left), treeHeight(tree, *right))
}

type treeStat struct {
	size uint64
}

func treeAndDbEqual(t *testing.T, tree *Tree, node Node, stat *treeStat) {
	dbNode, err := tree.kv.Get(node.nodeKey)
	if err != nil {
		t.Fatalf("error getting node from db: %s", err)
	}
	stat.size += node.sizeBytes()
	require.NoError(t, err)
	require.NotNil(t, dbNode)
	require.Equal(t, dbNode.nodeKey, node.nodeKey)
	require.Equal(t, dbNode.key, node.key)
	require.Equal(t, dbNode.hash, node.hash)
	require.Equal(t, dbNode.size, node.size)
	require.Equal(t, dbNode.subtreeHeight, node.subtreeHeight)
	if node.isLeaf() {
		return
	}
	require.Equal(t, dbNode.leftNodeKey, node.leftNodeKey)
	require.Equal(t, dbNode.rightNodeKey, node.rightNodeKey)

	leftNode := *node.left(tree)
	rightNode := *node.right(tree)
	treeAndDbEqual(t, tree, leftNode, stat)
	treeAndDbEqual(t, tree, rightNode, stat)
}

var osmoScalePath = fmt.Sprintf("%s/src/scratch/sqlite-osmo", os.Getenv("HOME"))

func TestBuild_OsmoScale(t *testing.T) {
	tmpDir := osmoScalePath

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)

	tree := &Tree{
		pool:           pool,
		metrics:        &metrics.TreeMetrics{},
		sql:            sql,
		cache:          NewNodeCache(),
		maxWorkingSize: 1024 * 1024 * 1024,
	}

	opts := testutil.OsmoLike()
	opts.Report = func() {
		tree.metrics.Report()
	}
	version1 := opts.Iterator.Nodes()
	i := 0
	start := time.Now()
	for ; version1.Valid(); err = version1.Next() {
		require.NoError(t, err)
		node := version1.GetNode()
		var keyBz bytes.Buffer
		keyBz.Write([]byte(node.StoreKey))
		keyBz.Write(node.Key)
		key := keyBz.Bytes()
		require.NoError(t, err)

		if node.Delete {
			t.Fatalf("unexpected delete in version 1")
		}

		_, err = tree.Set(key, node.Value)
		require.NoError(t, err)

		i++
		if i%500_000 == 0 {
			log.Info().Msgf("leaves=%s dur=%s; rate=%s",
				humanize.Comma(int64(i)),
				time.Since(start),
				humanize.Comma(int64(500_000/time.Since(start).Seconds())))
			start = time.Now()
		}
	}

	hash, _, err := tree.SaveVersion()
	require.NoError(t, err)
	fmt.Printf("version 1 hash: %x\n", hash)

	err = tree.LoadVersion(1)
	require.NoError(t, err)
	require.NoError(t, sql.Close())

	require.Equal(t, "bc4bc22437cc71b4ff8e6735ca27757b1bd6a6285c872bbf8d77007e864b5877",
		fmt.Sprintf("%x", hash))
}

func TestOsmoLike_HotStart(t *testing.T) {
	/* One big TODO for snapshot import
	tmpDir := "/tmp/iavl-init"

	pool := NewNodePool()
	sqlOpts := SqliteDbOptions{Path: tmpDir}
	tree := NewTree(sql, pool)

	opts := testutil.CompactedChangelogs("/Users/mattk/src/scratch/osmo-like/v2")
	root, err := sql.ImportSnapshot(1, false)

	require.NoError(t, tree.LoadVersion(1))
	require.NoError(t, err)
	tree.root = root

	require.NoError(t, sql.WarmLeaves())
	testTreeBuild(t, tree, opts)
	require.NoError(t, sql.Close())
	*/
}

func TestOsmoLike_ColdStart(t *testing.T) {
	tmpDir := "/tmp/iavl-init"

	sqlOpts := SqliteDbOptions{Path: tmpDir}
	opts := testutil.CompactedChangelogs("/Users/mattk/src/scratch/osmo-like/v2")

	testTreeBuild(t, sqlOpts, opts)
}

func TestTree_Import(t *testing.T) {
	tmpDir := "/Users/mattk/src/scratch/sqlite/height-zero"

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)

	root, err := sql.ImportSnapshot(1, true)
	require.NoError(t, err)
	require.NotNil(t, root)
}

func TestTree_Rehash(t *testing.T) {
	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: "/Users/mattk/src/scratch/sqlite/height-zero"})
	require.NoError(t, err)
	tree := NewTree(sql, pool)
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
				return NewTree(sql, pool)
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
				return NewTree(nil, pool)
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
