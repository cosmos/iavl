// TODO move to package iavl_test
// this means an audit of exported fields and types.
package iavl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

func MemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	s := fmt.Sprintf("alloc=%s sys=%s gc=%d",
		humanize.Bytes(m.HeapAlloc),
		//humanize.Bytes(m.TotalAlloc),
		humanize.Bytes(m.Sys),
		m.NumGC)
	return s
}

func testTreeBuild(t *testing.T, multiTree *MultiTree, opts *testutil.TreeBuildOptions) (cnt int64) {
	var (
		version int64
		err     error
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

	report := func() {
		dur := time.Since(since)

		var (
			workingBytes uint64
			workingSize  int64
			writeLeaves  int64
			writeTime    time.Duration
		)
		for _, tr := range multiTree.Trees {
			m := tr.sql.metrics
			workingBytes += tr.workingBytes
			workingSize += tr.workingSize
			writeLeaves += m.WriteLeaves
			writeTime += m.WriteTime
			m.WriteDurations = nil
			m.WriteLeaves = 0
			m.WriteTime = 0
		}
		fmt.Printf("leaves=%s time=%s last=%s μ=%s version=%d work-size=%s work-bytes=%s %s\n",
			humanize.Comma(cnt),
			dur.Round(time.Millisecond),
			humanize.Comma(int64(float64(sampleRate)/time.Since(since).Seconds())),
			humanize.Comma(int64(float64(cnt)/time.Since(itrStart).Seconds())),
			version,
			humanize.Comma(workingSize),
			humanize.Bytes(workingBytes),
			MemUsage())

		if writeTime > 0 {
			fmt.Printf("writes: cnt=%s wr/s=%s dur/wr=%s dur=%s\n",
				humanize.Comma(writeLeaves),
				humanize.Comma(int64(float64(writeLeaves)/writeTime.Seconds())),
				time.Duration(int64(writeTime)/writeLeaves),
				writeTime.Round(time.Millisecond),
			)
		}

		if err := multiTree.QueryReport(0); err != nil {
			t.Fatalf("query report err %v", err)
		}

		fmt.Println()

		since = time.Now()
	}

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
				report()
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
	require.Equal(t, version, opts.Until)
	require.Equal(t, opts.UntilHash, fmt.Sprintf("%x", multiTree.Hash()))
	return cnt
}

func TestTree_Hash(t *testing.T) {
	var err error

	tmpDir := t.TempDir()
	//tmpDir := "/tmp/iavl-test"
	t.Logf("levelDb tmpDir: %s\n", tmpDir)

	require.NoError(t, err)
	opts := testutil.BigTreeOptions_100_000()

	// this hash was validated as correct (with this same dataset) in iavl-bench
	// with `go run . tree --seed 1234 --dataset std`
	// at this commit tree: https://github.com/cosmos/iavl-bench/blob/3a6a1ec0a8cbec305e46239454113687da18240d/iavl-v0/main.go#L136
	opts.Until = 100
	opts.UntilHash = "0101e1d6f3158dcb7221acd7ed36ce19f2ef26847ffea7ce69232e362539e5cf"
	treeOpts := TreeOptions{CheckpointInterval: 10, HeightFilter: 1, StateStorage: true, EvictionDepth: 8}

	testStart := time.Now()
	multiTree := NewMultiTree(tmpDir, treeOpts)
	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
	require.True(t, ok)
	for _, sk := range itrs.StoreKeys() {
		require.NoError(t, multiTree.MountTree(sk))
	}
	leaves := testTreeBuild(t, multiTree, opts)
	treeDuration := time.Since(testStart)
	fmt.Printf("mean leaves/s: %s\n", humanize.Comma(int64(float64(leaves)/treeDuration.Seconds())))

	require.NoError(t, multiTree.Close())
}

func TestTree_Build_Load(t *testing.T) {
	// build the initial version of the tree with periodic checkpoints
	//tmpDir := t.TempDir()
	tmpDir := "/tmp/iavl-v2-test"
	opts := testutil.NewTreeBuildOptions().With10_000()
	multiTree := NewMultiTree(tmpDir, TreeOptions{CheckpointInterval: 4000, HeightFilter: 0, StateStorage: false})
	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
	require.True(t, ok)
	for _, sk := range itrs.StoreKeys() {
		require.NoError(t, multiTree.MountTree(sk))
	}
	t.Log("building initial tree to version 10,000")
	testTreeBuild(t, multiTree, opts)

	t.Log("snapshot tree at version 10,000")
	// take a snapshot at version 10,000
	require.NoError(t, multiTree.SnapshotConcurrently())
	require.NoError(t, multiTree.Close())

	t.Log("import snapshot into new tree")
	mt, err := ImportMultiTree(multiTree.pool, 10_000, tmpDir, DefaultTreeOptions())
	require.NoError(t, err)

	t.Log("build tree to version 12,000 and verify hash")
	require.NoError(t, opts.Iterator.Next())
	require.Equal(t, int64(10_001), opts.Iterator.Version())
	opts.Until = 12_000
	opts.UntilHash = "3a037f8dd67a5e1a9ef83a53b81c619c9ac0233abee6f34a400fb9b9dfbb4f8d"
	testTreeBuild(t, mt, opts)
	require.NoError(t, mt.Close())

	t.Log("export the tree at version 12,000 and import it into a sql db in pre-order")
	traverseOrder := PreOrder
	restorePreOrderMt := NewMultiTree(t.TempDir(), TreeOptions{CheckpointInterval: 4000})
	for sk, tree := range multiTree.Trees {
		require.NoError(t, restorePreOrderMt.MountTree(sk))
		exporter := tree.Export(traverseOrder)

		restoreTree := restorePreOrderMt.Trees[sk]
		_, err := restoreTree.sql.WriteSnapshot(context.Background(), tree.Version(), exporter.Next, SnapshotOptions{WriteCheckpoint: true, TraverseOrder: traverseOrder})
		require.NoError(t, err)
		require.NoError(t, restoreTree.LoadSnapshot(tree.Version(), traverseOrder))
	}
	require.NoError(t, restorePreOrderMt.Close())

	t.Log("export the tree at version 12,000 and import it into a sql db in post-order")
	traverseOrder = PostOrder
	restorePostOrderMt := NewMultiTree(t.TempDir(), TreeOptions{CheckpointInterval: 4000})
	for sk, tree := range multiTree.Trees {
		require.NoError(t, restorePostOrderMt.MountTree(sk))
		exporter := tree.Export(traverseOrder)

		restoreTree := restorePostOrderMt.Trees[sk]
		_, err := restoreTree.sql.WriteSnapshot(context.Background(), tree.Version(), exporter.Next, SnapshotOptions{WriteCheckpoint: true, TraverseOrder: traverseOrder})
		require.NoError(t, err)
		require.NoError(t, restoreTree.LoadSnapshot(tree.Version(), traverseOrder))
	}
	require.Equal(t, restorePostOrderMt.Hash(), restorePreOrderMt.Hash())

	t.Log("build tree to version 20,000 and verify hash")
	require.NoError(t, opts.Iterator.Next())
	require.Equal(t, int64(12_001), opts.Iterator.Version())
	opts.Until = 20_000
	opts.UntilHash = "25907b193c697903218d92fa70a87ef6cdd6fa5b9162d955a4d70a9d5d2c4824"
	testTreeBuild(t, restorePostOrderMt, opts)
	require.NoError(t, restorePostOrderMt.Close())
}

// pre-requisites for the 2 tests below:
// $ go run ./cmd gen tree --db /tmp/iavl-v2 --limit 1 --type osmo-like-many
// $ go run ./cmd snapshot --db /tmp/iavl-v2 --version 1
// mkdir -p /tmp/osmo-like-many/v2 && go run ./cmd gen emit --start 2 --limit 5000 --type osmo-like-many --out /tmp/osmo-like-many/v2
func TestOsmoLike_HotStart(t *testing.T) {
	tmpDir := "/tmp/iavl-v2"
	// logDir := "/tmp/osmo-like-many-v2"
	logDir := "/Users/mattk/src/scratch/osmo-like-many/v2"
	pool := NewNodePool()
	multiTree, err := ImportMultiTree(pool, 1, tmpDir, TreeOptions{
		HeightFilter:  1,
		StateStorage:  true,
		EvictionDepth: 16,
		MetricsProxy:  newPrometheusMetricsProxy(),
	})
	require.NoError(t, err)
	require.NotNil(t, multiTree)
	opts := testutil.CompactedChangelogs(logDir)
	opts.SampleRate = 250_000

	opts.Until = 1_000
	opts.UntilHash = "557663181d9ab97882ecfc6538e3b4cfe31cd805222fae905c4b4f4403ca5cda"

	testTreeBuild(t, multiTree, opts)
}

func TestOsmoLike_ColdStart(t *testing.T) {
	tmpDir := "/Users/mattk/.costor/iavl-v2"

	treeOpts := TreeOptions{
		CheckpointInterval: 240,
		StateStorage:       true,
		HeightFilter:       0,
		EvictionDepth:      30,
		MetricsProxy:       newPrometheusMetricsProxy(),
	}
	multiTree := NewMultiTree(tmpDir, treeOpts)
	require.NoError(t, multiTree.MountTrees())
	require.NoError(t, multiTree.LoadVersion(1))
	require.NoError(t, multiTree.WarmLeaves())

	logDir := "/tmp/osmo-like-many/v2"
	opts := testutil.CompactedChangelogs(logDir)
	opts.SampleRate = 250_000

	opts.Until = 500
	opts.UntilHash = "2670bd5767e70f2bf9e4f723b5f205759e39afdb5d8cfb6b54a4a3ecc27a1377"

	testTreeBuild(t, multiTree, opts)
}

func TestTree_Import(t *testing.T) {
	tmpDir := "/Users/mattk/src/scratch/sqlite/height-zero"

	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)

	root, err := sql.ImportSnapshotFromTable(1, PreOrder, true)
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
		node._hash()
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

func Test_EmptyTree(t *testing.T) {
	pool := NewNodePool()
	sql, err := NewInMemorySqliteDb(pool)
	require.NoError(t, err)
	tree := NewTree(sql, pool, TreeOptions{})

	_, err = tree.Set([]byte("foo"), []byte("bar"))
	require.NoError(t, err)
	_, err = tree.Set([]byte("baz"), []byte("qux"))
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	_, _, err = tree.Remove([]byte("foo"))
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	_, _, err = tree.Remove([]byte("baz"))
	require.NoError(t, err)
	hash, version, err := tree.SaveVersion()
	require.NoError(t, err)

	require.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hex.EncodeToString(sha256.New().Sum(nil)))
	require.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hex.EncodeToString(hash))

	err = tree.LoadVersion(version)
	require.NoError(t, err)
}

func Test_Replay_Tmp(t *testing.T) {
	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: "/Users/mattk/src/scratch/icahost"})
	require.NoError(t, err)
	tree := NewTree(sql, pool, TreeOptions{StateStorage: true})
	err = tree.LoadVersion(13946707)
	require.NoError(t, err)
}

func Test_Replay(t *testing.T) {
	unsafeBytesToStr := func(b []byte) string {
		return *(*string)(unsafe.Pointer(&b))
	}
	const versions = int64(1_000)
	gen := bench.ChangesetGenerator{
		StoreKey:         "replay",
		Seed:             1,
		KeyMean:          20,
		KeyStdDev:        3,
		ValueMean:        20,
		ValueStdDev:      3,
		InitialSize:      20,
		FinalSize:        500,
		Versions:         versions,
		ChangePerVersion: 10,
		DeleteFraction:   0.2,
	}
	itr, err := gen.Iterator()
	require.NoError(t, err)

	pool := NewNodePool()
	tmpDir := t.TempDir()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	tree := NewTree(sql, pool, TreeOptions{StateStorage: true, CheckpointInterval: 100})

	// we must buffer all sets/deletes and order them first for replay to work properly.
	// store v1 and v2 already do this via cachekv write buffering.
	// from cachekv a nil value is treated as a deletion; it is a domain requirement of the SDK that nil values are disallowed
	// since from the perspective of the cachekv they are indistinguishable from a deletion.

	ingest := func(start, last int64) {
		for ; itr.Valid(); err = itr.Next() {
			if itr.Version() > last {
				break
			}
			require.NoError(t, err)
			changeset := itr.Nodes()
			cache := make(map[string]*api.Node)
			for ; changeset.Valid(); err = changeset.Next() {
				require.NoError(t, err)
				node := changeset.GetNode()
				if itr.Version() < start {
					continue
				}
				if !node.Delete {
					// merge multiple sets into one set
					cache[unsafeBytesToStr(node.Key)] = node
				} else {
					cache[unsafeBytesToStr(node.Key)] = nil
				}
			}
			keys := make([]string, 0, len(cache))
			for k := range cache {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				node := cache[k]
				if node == nil {
					_, _, err := tree.Remove([]byte(k))
					require.NoError(t, err)
				} else {
					_, err := tree.Set([]byte(k), node.Value)
					require.NoError(t, err)
				}
			}

			if len(cache) > 0 {
				_, v, err := tree.SaveVersion()
				fmt.Printf("version=%d, hash=%x\n", v, tree.Hash())
				require.NoError(t, err)
			}
		}

		require.NoError(t, tree.Close())
	}

	ingest(1, 150)

	sql, err = NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	tree = NewTree(sql, pool, TreeOptions{StateStorage: true})
	err = tree.LoadVersion(140)
	require.NoError(t, err)
	itr, err = gen.Iterator()
	require.NoError(t, err)
	ingest(141, 170)

	sql, err = NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	tree = NewTree(sql, pool, TreeOptions{StateStorage: true, CheckpointInterval: 100})
	err = tree.LoadVersion(170)
	require.NoError(t, err)
	itr, err = gen.Iterator()
	require.NoError(t, err)
	ingest(171, 250)

	//sql, err = NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	//require.NoError(t, err)
	//tree = NewTree(sql, pool, TreeOptions{StateStorage: true})
	//require.NoError(t, err)
	//require.NoError(t, tree.Close())
	//
	//sql, err = NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	//require.NoError(t, err)
	//tree = NewTree(sql, pool, TreeOptions{StateStorage: true})
	//err = tree.LoadVersion(5)
	//require.NoError(t, err)
	//
	//tree = NewTree(sql, pool, TreeOptions{StateStorage: true})
	//err = tree.LoadVersion(555)
	//require.NoError(t, err)
	//
	//tree = NewTree(sql, pool, TreeOptions{StateStorage: true})
	//err = tree.LoadVersion(1000)
	//require.NoError(t, err)
}

func Test_Prune_Logic(t *testing.T) {
	const versions = int64(1_000)
	gen := bench.ChangesetGenerator{
		StoreKey:         "replay",
		Seed:             1,
		KeyMean:          20,
		KeyStdDev:        3,
		ValueMean:        20,
		ValueStdDev:      3,
		InitialSize:      20,
		FinalSize:        500,
		Versions:         versions,
		ChangePerVersion: 10,
		DeleteFraction:   0.2,
	}
	itr, err := gen.Iterator()
	require.NoError(t, err)

	pool := NewNodePool()
	// tmpDir := "/tmp/prune-logic"
	tmpDir := t.TempDir()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir, ShardTrees: false})
	require.NoError(t, err)
	tree := NewTree(sql, pool, TreeOptions{StateStorage: true, CheckpointInterval: 100})

	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		changeset := itr.Nodes()
		for ; changeset.Valid(); err = changeset.Next() {
			require.NoError(t, err)
			node := changeset.GetNode()
			if node.Delete {
				_, _, err := tree.Remove(node.Key)
				require.NoError(t, err)
			} else {
				_, err := tree.Set(node.Key, node.Value)
				require.NoError(t, err)
			}
		}
		_, version, err := tree.SaveVersion()
		// fmt.Printf("version=%d, hash=%x\n", version, tree.Hash())
		switch version {
		case 30:
			require.NoError(t, tree.DeleteVersionsTo(20))
		case 100:
			require.NoError(t, tree.DeleteVersionsTo(100))
		case 150:
			require.NoError(t, tree.DeleteVersionsTo(140))
		case 650:
			require.NoError(t, tree.DeleteVersionsTo(650))
		}
		require.NoError(t, err)
	}
}

func Test_Prune_Performance(t *testing.T) {
	tmpDir := "/tmp/iavl-v2"

	multiTree := NewMultiTree(tmpDir, TreeOptions{CheckpointInterval: 50, StateStorage: false})
	require.NoError(t, multiTree.MountTrees())
	require.NoError(t, multiTree.LoadVersion(1))
	require.NoError(t, multiTree.WarmLeaves())

	// logDir := "/tmp/osmo-like-many-v2"
	opts := testutil.CompactedChangelogs("/Users/mattk/src/scratch/osmo-like-many/v2")
	opts.SampleRate = 250_000

	opts.Until = 1_000
	opts.UntilHash = "557663181d9ab97882ecfc6538e3b4cfe31cd805222fae905c4b4f4403ca5cda"

	itr := opts.Iterator
	var (
		err       error
		cnt       int64
		version   int64
		since     = time.Now()
		itrStart  = time.Now()
		lastPrune = 1
	)
	report := func() {
		dur := time.Since(since)

		var (
			workingBytes uint64
			workingSize  int64
			writeLeaves  int64
			writeTime    time.Duration
		)
		for _, tr := range multiTree.Trees {
			m := tr.sql.metrics
			workingBytes += tr.workingBytes
			workingSize += tr.workingSize
			writeLeaves += m.WriteLeaves
			writeTime += m.WriteTime
			m.WriteDurations = nil
			m.WriteLeaves = 0
			m.WriteTime = 0
		}
		fmt.Printf("leaves=%s time=%s last=%s μ=%s version=%d work-bytes=%s work-size=%s %s\n",
			humanize.Comma(cnt),
			dur.Round(time.Millisecond),
			humanize.Comma(int64(float64(opts.SampleRate)/time.Since(since).Seconds())),
			humanize.Comma(int64(float64(cnt)/time.Since(itrStart).Seconds())),
			version,
			humanize.Bytes(workingBytes),
			humanize.Comma(workingSize),
			MemUsage())

		if writeTime > 0 {
			fmt.Printf("writes: cnt=%s wr/s=%s dur/wr=%s dur=%s\n",
				humanize.Comma(writeLeaves),
				humanize.Comma(int64(float64(writeLeaves)/writeTime.Seconds())),
				time.Duration(int64(writeTime)/writeLeaves),
				writeTime.Round(time.Millisecond),
			)
		}

		if err := multiTree.QueryReport(0); err != nil {
			t.Fatalf("query report err %v", err)
		}

		fmt.Println()

		since = time.Now()
	}

	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		changeset := itr.Nodes()
		for ; changeset.Valid(); err = changeset.Next() {
			cnt++
			require.NoError(t, err)
			node := changeset.GetNode()
			key := node.Key

			tree, ok := multiTree.Trees[node.StoreKey]
			require.True(t, ok)

			if !node.Delete {
				_, err = tree.Set(key, node.Value)
				require.NoError(t, err)
			} else {
				_, _, err := tree.Remove(key)
				require.NoError(t, err)
			}

			if cnt%opts.SampleRate == 0 {
				report()
			}
		}

		_, version, err = multiTree.SaveVersionConcurrently()
		require.NoError(t, err)

		require.NoError(t, err)
		if version == opts.Until {
			break
		}

		lastPrune++
		// trigger two prunes close together in order to test the receipt of a prune signal before a previous prune has completed
		if lastPrune == 80 || lastPrune == 85 {
			pruneTo := version - 1
			t.Logf("prune to version %d", pruneTo)
			for _, tree := range multiTree.Trees {
				require.NoError(t, tree.DeleteVersionsTo(pruneTo))
			}
			t.Log("prune signals sent")
			if lastPrune == 85 {
				lastPrune = 0
			}
		}
	}
}

var _ metrics.Proxy = &prometheusMetricsProxy{}

type prometheusMetricsProxy struct {
	workingSize  prometheus.Gauge
	workingBytes prometheus.Gauge
}

func newPrometheusMetricsProxy() *prometheusMetricsProxy {
	p := &prometheusMetricsProxy{}
	p.workingSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "iavl_working_size",
		Help: "working size",
	})
	p.workingBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "iavl_working_bytes",
		Help: "working bytes",
	})
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(":2112", nil)
		if err != nil {
			panic(err)
		}
	}()
	return p
}

func (p *prometheusMetricsProxy) IncrCounter(_ float32, _ ...string) {
}

func (p *prometheusMetricsProxy) SetGauge(val float32, keys ...string) {
	k := keys[1]
	switch k {
	case "working_size":
		p.workingSize.Set(float64(val))
	case "working_bytes":
		p.workingBytes.Set(float64(val))
	}
}

func (p *prometheusMetricsProxy) MeasureSince(_ time.Time, _ ...string) {}
