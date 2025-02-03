// TODO move to package iavl_test
// this means an audit of exported fields and types.
package iavl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"
	"github.com/stretchr/testify/require"
)

func TestTree_Hash(t *testing.T) {
	var err error
	tmpDir := t.TempDir()

	require.NoError(t, err)
	opts := testutil.BigTreeOptions100_000()

	// this hash was validated as correct (with this same dataset) in iavl-bench
	// with `go run . tree --seed 1234 --dataset std`
	// at this commit tree: https://github.com/cosmos/iavl-bench/blob/3a6a1ec0a8cbec305e46239454113687da18240d/iavl-v0/main.go#L136
	opts.Until = 100
	opts.UntilHash = "0101e1d6f3158dcb7221acd7ed36ce19f2ef26847ffea7ce69232e362539e5cf"
	treeOpts := TreeOptions{
		CheckpointInterval: 10, HeightFilter: 1, StateStorage: true, EvictionDepth: 8,
		PruneRatio: 0.5, MinimumKeepVersions: 20,
	}

	testStart := time.Now()
	multiTree := NewMultiTree(tmpDir, treeOpts)
	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
	require.True(t, ok)
	for _, sk := range itrs.StoreKeys() {
		require.NoError(t, multiTree.MountTree(sk))
	}
	leaves := multiTree.TestBuild(t, opts)
	treeDuration := time.Since(testStart)
	fmt.Printf("mean leaves/s: %s\n", humanize.Comma(int64(float64(leaves)/treeDuration.Seconds())))

	require.NoError(t, multiTree.Close())
}

func TestTree_Rehash(t *testing.T) {
	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: t.TempDir()})
	require.NoError(t, err)
	tree := NewTree(sql, pool, DefaultTreeOptions())
	opts := testutil.BigTreeOptions100_000()
	err = tree.LoadVersion(0)
	require.NoError(t, err)

	itr := opts.Iterator
	// save 1 version
	for ; itr.Valid(); err = itr.Next() {
		if itr.Version() > 1 {
			break
		}
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
	}
	_, v, err := tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, int64(1), v)

	savedHash := make([]byte, 32)
	n := copy(savedHash, tree.root.hash)
	require.Equal(t, 32, n)
	var step func(node *Node)
	cf := sql.readConnectionFactory()
	step = func(node *Node) {
		if node.isLeaf() {
			return
		}
		node.hash = nil
		step(node.left(tree.sql, cf))
		step(node.right(tree.sql, cf))
		node._hash()
	}
	step(tree.root)
	require.Equal(t, savedHash, tree.root.hash)
}

func TestTree_Build_Load(t *testing.T) {
	// build the initial version of the tree with periodic checkpoints
	tmpDir := t.TempDir()
	opts := testutil.NewTreeBuildOptions().With10_000()
	mt := NewMultiTree(tmpDir, TreeOptions{CheckpointInterval: 4000, HeightFilter: 0, StateStorage: false})
	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
	require.True(t, ok)
	for _, sk := range itrs.StoreKeys() {
		require.NoError(t, mt.MountTree(sk))
	}
	t.Log("building initial tree to version 10,000")
	mt.TestBuild(t, opts)

	t.Log("snapshot tree at version 10,000")
	// take a snapshot at version 10,000
	require.NoError(t, mt.SnapshotConcurrently())
	require.NoError(t, mt.Close())

	t.Log("import snapshot into new tree")
	mt, err := ImportMultiTree(mt.pool, 10_000, tmpDir, DefaultTreeOptions())
	require.NoError(t, err)

	t.Log("build tree to version 12,000 and verify hash")
	require.NoError(t, opts.Iterator.Next())
	require.Equal(t, int64(10_001), opts.Iterator.Version())
	opts.Until = 12_000
	opts.UntilHash = "3a037f8dd67a5e1a9ef83a53b81c619c9ac0233abee6f34a400fb9b9dfbb4f8d"
	mt.TestBuild(t, opts)
	require.NoError(t, mt.Close())
}

// func TestOsmoLike_HotStart(t *testing.T) {
// 	tmpDir := "/Users/mattk/.costor/iavl-v2"
// 	logDir := "/Users/mattk/src/devmos/osmo-like-many/v2"

// 	pool := NewNodePool()
// 	multiTree, err := ImportMultiTree(pool, 1, tmpDir, TreeOptions{
// 		HeightFilter:       0,
// 		StateStorage:       false,
// 		CheckpointInterval: 1001,
// 	})
// 	require.NoError(t, err)
// 	require.NotNil(t, multiTree)
// 	opts := testutil.CompactedChangelogs(logDir)
// 	opts.SampleRate = 250_000

// 	// opts.Until = 1_000
// 	// opts.UntilHash = "557663181d9ab97882ecfc6538e3b4cfe31cd805222fae905c4b4f4403ca5cda"
// 	opts.Until = 500
// 	opts.UntilHash = "2670bd5767e70f2bf9e4f723b5f205759e39afdb5d8cfb6b54a4a3ecc27a1377"

// 	multiTree.TestBuild(t, opts)
// }

// func TestOsmoLike_ColdStart(t *testing.T) {
// 	tmpDir := "/tmp/iavl-v2"
// 	logDir := "/Users/mattk/src/devmos/osmo-like-many/v2"

// 	treeOpts := DefaultTreeOptions()
// 	treeOpts.CheckpointInterval = 50
// 	// treeOpts.CheckpointMemory = 1.5 * 1024 * 1024 * 1024
// 	treeOpts.StateStorage = true
// 	treeOpts.HeightFilter = 1
// 	// treeOpts.EvictionDepth = 22
// 	treeOpts.MetricsProxy = newPrometheusMetricsProxy()
// 	multiTree := NewMultiTree(tmpDir, treeOpts)
// 	require.NoError(t, multiTree.MountTrees())
// 	require.NoError(t, multiTree.LoadVersion(1))
// 	require.NoError(t, multiTree.WarmLeaves())

// 	opts := testutil.CompactedChangelogs(logDir)
// 	opts.SampleRate = 250_000

// 	// opts.Until = 1_000
// 	// opts.UntilHash = "557663181d9ab97882ecfc6538e3b4cfe31cd805222fae905c4b4f4403ca5cda"
// 	opts.Until = 500
// 	opts.UntilHash = "2670bd5767e70f2bf9e4f723b5f205759e39afdb5d8cfb6b54a4a3ecc27a1377"

// 	multiTree.TestBuild(t, opts)
// }

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
				sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: t.TempDir()})
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
				rehashTree(tree.stagedRoot)
				tree.stagedVersion++
				return tree.stagedRoot.hash
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
					require.Equal(t, int64(104938), tree.stagedRoot.size)
					fmt.Printf("version=%d, hash=%x size=%d\n", itr.Version(), h, tree.stagedRoot.size)
				case 150:
					h := tc.hashFn(tree)
					require.Equal(t, "04c42dd1cec683cbbd4974027e4b003b848e389a33d03d7a9105183e6d108dd9", hex.EncodeToString(h))
					require.Equal(t, int64(105030), tree.stagedRoot.size)
					fmt.Printf("version=%d, hash=%x size=%d\n", itr.Version(), h, tree.stagedRoot.size)
				}
			}
		})
	}
}

func Test_TrivialTree(t *testing.T) {
	pool := NewNodePool()
	tmpDir := t.TempDir()

	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	tree := NewTree(sql, pool, TreeOptions{})

	gen := bench.ChangesetGenerator{
		StoreKey:         "bank",
		Seed:             1234,
		KeyMean:          56,
		KeyStdDev:        3,
		ValueMean:        100,
		ValueStdDev:      1200,
		InitialSize:      10,
		FinalSize:        25,
		Versions:         10,
		ChangePerVersion: 3,
		DeleteFraction:   0,
	}

	itr, err := gen.Iterator()
	require.NoError(t, err)
	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		changeset := itr.Nodes()
		v := itr.Version()
		fmt.Printf("version=%d\n", v)
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
		require.NoError(t, err)
		_, _, err = tree.SaveVersion()
		require.NoError(t, err)
	}
}

func Test_EmptyTree(t *testing.T) {
	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: t.TempDir()})
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

func Test_Replay(t *testing.T) {
	unsafeBytesToStr := func(b []byte) string {
		return *(*string)(unsafe.Pointer(&b))
	}
	const versions = int64(1_000)
	hashes := make([][]byte, versions+1)
	orphanCounts := make([]int64, versions+1)
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

	// tmpDir := t.TempDir()
	tmpDir := "/tmp/replay"
	require.NoError(t, os.RemoveAll(tmpDir))
	require.NoError(t, os.Mkdir(tmpDir, 0o0755))

	sql, err := NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	tree := NewTree(sql, pool, TreeOptions{
		StateStorage: true, CheckpointInterval: 53, MinimumKeepVersions: 1000,
	})

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
				hash, v, err := tree.SaveVersion()
				require.NoError(t, err)
				require.NotNil(t, hash)
				fmt.Printf("version=%d hash=%x orphans=%d\n", v, hash, tree.orphanBranchCount)
				if hashes[v] == nil {
					hashes[v] = hash
					orphanCounts[v] = tree.orphanBranchCount
				} else {
					require.Equal(t, hashes[v], hash)
					// this count will desync if check point boundaries are not consistent
					// require.Equal(t, orphanCounts[v], tree.orphanBranchCount)
				}
			}
		}

		require.NoError(t, tree.Close())
	}

	ingest(1, 300)

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
	tree = NewTree(sql, pool, TreeOptions{StateStorage: true, CheckpointInterval: 100, PruneRatio: 1})
	err = tree.LoadVersion(170)
	require.NoError(t, err)
	itr, err = gen.Iterator()
	require.NoError(t, err)
	ingest(171, 1000)

	// a prune should have occurred at version 801, replay from boundary back up to 1000
	sql, err = NewSqliteDb(pool, SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	tree = NewTree(sql, pool, TreeOptions{StateStorage: true, CheckpointInterval: 100})
	err = tree.LoadVersion(701)
	require.ErrorContains(t, err, "too old; prior checkpoint not found")
	err = tree.LoadVersion(807)
	require.NoError(t, err)
	itr, err = gen.Iterator()
	require.NoError(t, err)
	ingest(808, 1000)
}
