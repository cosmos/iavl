package iavl

import (
	"bytes"
	"flag"
	"math/rand"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tmlibs/db"

	cmn "github.com/tendermint/tmlibs/common"
)

var testLevelDB bool
var testFuzzIterations int

func init() {
	flag.BoolVar(&testLevelDB, "test.leveldb", false, "test leveldb backend")
	flag.IntVar(&testFuzzIterations, "test.fuzz-iterations", 100000, "number of fuzz testing iterations")
	flag.Parse()
}

func getTestDB() (db.DB, func()) {
	if testLevelDB {
		d, err := db.NewGoLevelDB("test", ".")
		if err != nil {
			panic(err)
		}
		return d, func() {
			d.Close()
			os.RemoveAll("./test.db")
		}
	}
	return db.NewMemDB(), func() {}
}

func TestVersionedRandomTree(t *testing.T) {
	require := require.New(t)

	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(d, 100)
	versions := 50
	keysPerVersion := 30

	// Create a tree of size 1000 with 100 versions.
	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			k := []byte(cmn.RandStr(8))
			v := []byte(cmn.RandStr(8))
			tree.Set(k, v)
		}
		tree.SaveVersion()
	}
	require.Equal(versions, len(tree.ndb.roots()), "wrong number of roots")
	require.Equal(versions*keysPerVersion, len(tree.ndb.leafNodes()), "wrong number of nodes")

	// Before deleting old versions, we should have equal or more nodes in the
	// db than in the current tree version.
	require.True(len(tree.ndb.nodes()) >= tree.nodeSize())

	for i := 1; i < versions; i++ {
		tree.DeleteVersion(int64(i))
	}

	require.Len(tree.versions, 1, "tree must have one version left")
	require.Equal(tree.versions[int64(versions)].root, tree.root)

	// After cleaning up all previous versions, we should have as many nodes
	// in the db as in the current tree version.
	require.Len(tree.ndb.leafNodes(), tree.Size())

	require.Equal(tree.nodeSize(), len(tree.ndb.nodes()))
}

func TestVersionedRandomTreeSmallKeys(t *testing.T) {
	require := require.New(t)
	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(d, 100)
	singleVersionTree := NewVersionedTree(db.NewMemDB(), 0)
	versions := 20
	keysPerVersion := 50

	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			// Keys of size one are likely to be overwritten.
			k := []byte(cmn.RandStr(1))
			v := []byte(cmn.RandStr(8))
			tree.Set(k, v)
			singleVersionTree.Set(k, v)
		}
		tree.SaveVersion()
	}
	singleVersionTree.SaveVersion()

	for i := 1; i < versions; i++ {
		tree.DeleteVersion(int64(i))
	}

	// After cleaning up all previous versions, we should have as many nodes
	// in the db as in the current tree version. The simple tree must be equal
	// too.
	require.Len(tree.ndb.leafNodes(), tree.Size())
	require.Len(tree.ndb.nodes(), tree.nodeSize())
	require.Len(tree.ndb.nodes(), singleVersionTree.nodeSize())

	// Try getting random keys.
	for i := 0; i < keysPerVersion; i++ {
		_, val := tree.Get([]byte(cmn.RandStr(1)))
		require.NotNil(val)
		require.NotEmpty(val)
	}
}

func TestVersionedRandomTreeSmallKeysRandomDeletes(t *testing.T) {
	require := require.New(t)
	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(d, 100)
	singleVersionTree := NewVersionedTree(db.NewMemDB(), 0)
	versions := 30
	keysPerVersion := 50

	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			// Keys of size one are likely to be overwritten.
			k := []byte(cmn.RandStr(1))
			v := []byte(cmn.RandStr(8))
			tree.Set(k, v)
			singleVersionTree.Set(k, v)
		}
		tree.SaveVersion()
	}
	singleVersionTree.SaveVersion()

	for _, i := range rand.Perm(versions - 1) {
		tree.DeleteVersion(int64(i + 1))
	}

	// After cleaning up all previous versions, we should have as many nodes
	// in the db as in the current tree version. The simple tree must be equal
	// too.
	require.Len(tree.ndb.leafNodes(), tree.Size())
	require.Len(tree.ndb.nodes(), tree.nodeSize())
	require.Len(tree.ndb.nodes(), singleVersionTree.nodeSize())

	// Try getting random keys.
	for i := 0; i < keysPerVersion; i++ {
		_, val := tree.Get([]byte(cmn.RandStr(1)))
		require.NotNil(val)
		require.NotEmpty(val)
	}
}

func TestVersionedTreeSpecial1(t *testing.T) {
	tree := NewVersionedTree(db.NewMemDB(), 100)

	tree.Set([]byte("C"), []byte("so43QQFN"))
	tree.SaveVersion()

	tree.Set([]byte("A"), []byte("ut7sTTAO"))
	tree.SaveVersion()

	tree.Set([]byte("X"), []byte("AoWWC1kN"))
	tree.SaveVersion()

	tree.Set([]byte("T"), []byte("MhkWjkVy"))
	tree.SaveVersion()

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)
	tree.DeleteVersion(3)

	require.Equal(t, tree.nodeSize(), len(tree.ndb.nodes()))
}

func TestVersionedRandomTreeSpecial2(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 100)

	tree.Set([]byte("OFMe2Yvm"), []byte("ez2OtQtE"))
	tree.Set([]byte("WEN4iN7Y"), []byte("kQNyUalI"))
	tree.SaveVersion()

	tree.Set([]byte("1yY3pXHr"), []byte("udYznpII"))
	tree.Set([]byte("7OSHNE7k"), []byte("ff181M2d"))
	tree.SaveVersion()

	tree.DeleteVersion(1)
	require.Len(tree.ndb.nodes(), tree.nodeSize())
}

func TestVersionedEmptyTree(t *testing.T) {
	require := require.New(t)
	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(d, 0)

	hash, v, err := tree.SaveVersion()
	require.Nil(hash)
	require.EqualValues(1, v)
	require.NoError(err)

	hash, v, err = tree.SaveVersion()
	require.Nil(hash)
	require.EqualValues(2, v)
	require.NoError(err)

	hash, v, err = tree.SaveVersion()
	require.Nil(hash)
	require.EqualValues(3, v)
	require.NoError(err)

	hash, v, err = tree.SaveVersion()
	require.Nil(hash)
	require.EqualValues(4, v)
	require.NoError(err)

	require.EqualValues(4, tree.Version())

	require.True(tree.VersionExists(1))
	require.True(tree.VersionExists(3))

	require.NoError(tree.DeleteVersion(1))
	require.NoError(tree.DeleteVersion(3))

	require.False(tree.VersionExists(1))
	require.False(tree.VersionExists(3))

	tree.Set([]byte("k"), []byte("v"))
	require.EqualValues(5, tree.root.version)

	// Now reload the tree.

	tree = NewVersionedTree(d, 0)
	tree.Load()

	require.False(tree.VersionExists(1))
	require.True(tree.VersionExists(2))
	require.False(tree.VersionExists(3))

	require.Empty(tree.versions[2].root)
}

func TestVersionedTree(t *testing.T) {
	require := require.New(t)
	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(d, 0)

	// We start with zero keys in the databse.
	require.Equal(0, tree.ndb.size())
	require.True(tree.IsEmpty())

	// version 0

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))

	// Still zero keys, since we haven't written them.
	require.Len(tree.ndb.leafNodes(), 0)
	require.False(tree.IsEmpty())

	// Now let's write the keys to storage.
	hash1, v, err := tree.SaveVersion()
	require.NoError(err)
	require.False(tree.IsEmpty())
	require.EqualValues(1, v)

	// -----1-----
	// key1 = val0  version=1
	// key2 = val0  version=1
	// key2 (root)  version=1
	// -----------

	nodes1 := tree.ndb.leafNodes()
	require.Len(nodes1, 2, "db should have a size of 2")

	// version  1

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.Set([]byte("key3"), []byte("val1"))
	require.Len(tree.ndb.leafNodes(), len(nodes1))

	hash2, v2, err := tree.SaveVersion()
	require.NoError(err)
	require.False(bytes.Equal(hash1, hash2))
	require.EqualValues(v+1, v2)

	// Recreate a new tree and load it, to make sure it works in this
	// scenario.
	tree = NewVersionedTree(d, 100)
	_, err = tree.Load()
	require.NoError(err)

	require.Len(tree.versions, 2, "wrong number of versions")
	require.EqualValues(v2, tree.Version())

	// -----1-----
	// key1 = val0  <orphaned>
	// key2 = val0  <orphaned>
	// -----2-----
	// key1 = val1
	// key2 = val1
	// key3 = val1
	// -----------

	nodes2 := tree.ndb.leafNodes()
	require.Len(nodes2, 5, "db should have grown in size")
	require.Len(tree.ndb.orphans(), 3, "db should have three orphans")

	// Create two more orphans.
	tree.Remove([]byte("key1"))
	tree.Set([]byte("key2"), []byte("val2"))

	hash3, v3, _ := tree.SaveVersion()
	require.EqualValues(3, v3)

	// -----1-----
	// key1 = val0  <orphaned> (replaced)
	// key2 = val0  <orphaned> (replaced)
	// -----2-----
	// key1 = val1  <orphaned> (removed)
	// key2 = val1  <orphaned> (replaced)
	// key3 = val1
	// -----3-----
	// key2 = val2
	// -----------

	nodes3 := tree.ndb.leafNodes()
	require.Len(nodes3, 6, "wrong number of nodes")
	require.Len(tree.ndb.orphans(), 6, "wrong number of orphans")

	hash4, _, _ := tree.SaveVersion()
	require.EqualValues(hash3, hash4)
	require.NotNil(hash4)

	tree = NewVersionedTree(d, 100)
	_, err = tree.Load()
	require.NoError(err)

	// ------------
	// DB UNCHANGED
	// ------------

	nodes4 := tree.ndb.leafNodes()
	require.Len(nodes4, len(nodes3), "db should not have changed in size")

	tree.Set([]byte("key1"), []byte("val0"))

	// "key2"
	_, val := tree.GetVersioned([]byte("key2"), 0)
	require.Nil(val)

	_, val = tree.GetVersioned([]byte("key2"), 1)
	require.Equal("val0", string(val))

	_, val = tree.GetVersioned([]byte("key2"), 2)
	require.Equal("val1", string(val))

	_, val = tree.Get([]byte("key2"))
	require.Equal("val2", string(val))

	// "key1"
	_, val = tree.GetVersioned([]byte("key1"), 1)
	require.Equal("val0", string(val))

	_, val = tree.GetVersioned([]byte("key1"), 2)
	require.Equal("val1", string(val))

	_, val = tree.GetVersioned([]byte("key1"), 3)
	require.Nil(val)

	_, val = tree.GetVersioned([]byte("key1"), 4)
	require.Nil(val)

	_, val = tree.Get([]byte("key1"))
	require.Equal("val0", string(val))

	// "key3"
	_, val = tree.GetVersioned([]byte("key3"), 0)
	require.Nil(val)

	_, val = tree.GetVersioned([]byte("key3"), 2)
	require.Equal("val1", string(val))

	_, val = tree.GetVersioned([]byte("key3"), 3)
	require.Equal("val1", string(val))

	// Delete a version. After this the keys in that version should not be found.

	tree.DeleteVersion(2)

	// -----1-----
	// key1 = val0
	// key2 = val0
	// -----2-----
	// key3 = val1
	// -----3-----
	// key2 = val2
	// -----------

	nodes5 := tree.ndb.leafNodes()
	require.True(len(nodes5) < len(nodes4), "db should have shrunk after delete %d !< %d", len(nodes5), len(nodes4))

	_, val = tree.GetVersioned([]byte("key2"), 2)
	require.Nil(val)

	_, val = tree.GetVersioned([]byte("key3"), 2)
	require.Nil(val)

	// But they should still exist in the latest version.

	_, val = tree.Get([]byte("key2"))
	require.Equal("val2", string(val))

	_, val = tree.Get([]byte("key3"))
	require.Equal("val1", string(val))

	// Version 1 should still be available.

	_, val = tree.GetVersioned([]byte("key1"), 1)
	require.Equal("val0", string(val))

	_, val = tree.GetVersioned([]byte("key2"), 1)
	require.Equal("val0", string(val))
}

func TestVersionedTreeVersionDeletingEfficiency(t *testing.T) {
	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(d, 0)

	tree.Set([]byte("key0"), []byte("val0"))
	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))
	tree.SaveVersion()

	require.Len(t, tree.ndb.leafNodes(), 3)

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.Set([]byte("key3"), []byte("val1"))
	tree.SaveVersion()

	require.Len(t, tree.ndb.leafNodes(), 6)

	tree.Set([]byte("key0"), []byte("val2"))
	tree.Remove([]byte("key1"))
	tree.Set([]byte("key2"), []byte("val2"))
	tree.SaveVersion()

	require.Len(t, tree.ndb.leafNodes(), 8)

	tree.DeleteVersion(2)

	require.Len(t, tree.ndb.leafNodes(), 6)

	tree.DeleteVersion(1)

	require.Len(t, tree.ndb.leafNodes(), 3)

	tree2 := NewVersionedTree(db.NewMemDB(), 0)
	tree2.Set([]byte("key0"), []byte("val2"))
	tree2.Set([]byte("key2"), []byte("val2"))
	tree2.Set([]byte("key3"), []byte("val1"))
	tree2.SaveVersion()

	require.Equal(t, tree2.nodeSize(), tree.nodeSize())
}

func TestVersionedTreeOrphanDeleting(t *testing.T) {
	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("key0"), []byte("val0"))
	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))
	tree.SaveVersion()

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.Set([]byte("key3"), []byte("val1"))
	tree.SaveVersion()

	tree.Set([]byte("key0"), []byte("val2"))
	tree.Remove([]byte("key1"))
	tree.Set([]byte("key2"), []byte("val2"))
	tree.SaveVersion()
	tree.DeleteVersion(2)

	_, val := tree.Get([]byte("key0"))
	require.Equal(t, val, []byte("val2"))

	_, val = tree.Get([]byte("key1"))
	require.Nil(t, val)

	_, val = tree.Get([]byte("key2"))
	require.Equal(t, val, []byte("val2"))

	_, val = tree.Get([]byte("key3"))
	require.Equal(t, val, []byte("val1"))

	tree.DeleteVersion(1)

	require.Len(t, tree.ndb.leafNodes(), 3)
}

func TestVersionedTreeSpecialCase(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 100)

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))
	tree.SaveVersion()

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.SaveVersion()

	tree.Set([]byte("key2"), []byte("val2"))
	tree.SaveVersion()

	tree.DeleteVersion(2)

	_, val := tree.GetVersioned([]byte("key2"), 1)
	require.Equal("val0", string(val))
}

func TestVersionedTreeSpecialCase2(t *testing.T) {
	require := require.New(t)
	d := db.NewMemDB()

	tree := NewVersionedTree(d, 100)

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))
	tree.SaveVersion()

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.SaveVersion()

	tree.Set([]byte("key2"), []byte("val2"))
	tree.SaveVersion()

	tree = NewVersionedTree(d, 100)
	_, err := tree.Load()
	require.NoError(err)

	require.NoError(tree.DeleteVersion(2))

	_, val := tree.GetVersioned([]byte("key2"), 1)
	require.Equal("val0", string(val))
}

func TestVersionedTreeSpecialCase3(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("m"), []byte("liWT0U6G"))
	tree.Set([]byte("G"), []byte("7PxRXwUA"))
	tree.SaveVersion()

	tree.Set([]byte("7"), []byte("XRLXgf8C"))
	tree.SaveVersion()

	tree.Set([]byte("r"), []byte("bBEmIXBU"))
	tree.SaveVersion()

	tree.Set([]byte("i"), []byte("kkIS35te"))
	tree.SaveVersion()

	tree.Set([]byte("k"), []byte("CpEnpzKJ"))
	tree.SaveVersion()

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)
	tree.DeleteVersion(3)
	tree.DeleteVersion(4)

	require.Equal(tree.nodeSize(), len(tree.ndb.nodes()))
}

func TestVersionedTreeSaveAndLoad(t *testing.T) {
	require := require.New(t)
	d := db.NewMemDB()
	tree := NewVersionedTree(d, 0)

	// Loading with an empty root is a no-op.
	tree.Load()

	tree.Set([]byte("C"), []byte("so43QQFN"))
	tree.SaveVersion()

	tree.Set([]byte("A"), []byte("ut7sTTAO"))
	tree.SaveVersion()

	tree.Set([]byte("X"), []byte("AoWWC1kN"))
	tree.SaveVersion()

	tree.SaveVersion()
	tree.SaveVersion()
	tree.SaveVersion()

	preHash := tree.Hash()
	require.NotNil(preHash)

	require.Equal(6, tree.Version())

	// Reload the tree, to test that roots and orphans are properly loaded.
	ntree := NewVersionedTree(d, 0)
	ntree.Load()

	require.False(ntree.IsEmpty())
	require.Equal(6, ntree.Version())

	postHash := ntree.Hash()
	require.Equal(preHash, postHash)

	ntree.Set([]byte("T"), []byte("MhkWjkVy"))
	ntree.SaveVersion()

	ntree.DeleteVersion(6)
	ntree.DeleteVersion(5)
	ntree.DeleteVersion(1)
	ntree.DeleteVersion(2)
	ntree.DeleteVersion(4)
	ntree.DeleteVersion(3)

	require.False(ntree.IsEmpty())
	require.Equal(4, ntree.Size())
	require.Len(ntree.ndb.nodes(), ntree.nodeSize())
}

func TestVersionedTreeErrors(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 100)

	// Can't delete non-existent versions.
	require.Error(tree.DeleteVersion(1))
	require.Error(tree.DeleteVersion(99))

	tree.Set([]byte("key"), []byte("val"))

	// Saving with content is ok.
	_, _, err := tree.SaveVersion()
	require.NoError(err)

	// Can't delete current version.
	require.Error(tree.DeleteVersion(1))

	// Trying to get a key from a version which doesn't exist.
	_, val := tree.GetVersioned([]byte("key"), 404)
	require.Nil(val)

	// Same thing with proof. We get an error because a proof couldn't be
	// constructed.
	val, proof, err := tree.GetVersionedWithProof([]byte("key"), 404)
	require.Nil(val)
	require.Nil(proof)
	require.Error(err)
}

func TestVersionedCheckpoints(t *testing.T) {
	require := require.New(t)
	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(d, 100)
	versions := 50
	keysPerVersion := 10
	versionsPerCheckpoint := 5
	keys := map[int64]([][]byte){}

	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			k := []byte(cmn.RandStr(1))
			v := []byte(cmn.RandStr(8))
			keys[int64(i)] = append(keys[int64(i)], k)
			tree.Set(k, v)
		}
		tree.SaveVersion()
	}

	for i := 1; i <= versions; i++ {
		if i%versionsPerCheckpoint != 0 {
			tree.DeleteVersion(int64(i))
		}
	}

	// Make sure all keys exist at least once.
	for _, ks := range keys {
		for _, k := range ks {
			_, val := tree.Get(k)
			require.NotEmpty(val)
		}
	}

	// Make sure all keys from deleted versions aren't present.
	for i := 1; i <= versions; i++ {
		if i%versionsPerCheckpoint != 0 {
			for _, k := range keys[int64(i)] {
				_, val := tree.GetVersioned(k, int64(i))
				require.Nil(val)
			}
		}
	}

	// Make sure all keys exist at all checkpoints.
	for i := 1; i <= versions; i++ {
		for _, k := range keys[int64(i)] {
			if i%versionsPerCheckpoint == 0 {
				_, val := tree.GetVersioned(k, int64(i))
				require.NotEmpty(val)
			}
		}
	}
}

func TestVersionedCheckpointsSpecialCase(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 0)
	key := []byte("k")

	tree.Set(key, []byte("val1"))

	tree.SaveVersion()
	// ...
	tree.SaveVersion()
	// ...
	tree.SaveVersion()
	// ...
	// This orphans "k" at version 1.
	tree.Set(key, []byte("val2"))
	tree.SaveVersion()

	// When version 1 is deleted, the orphans should move to the next
	// checkpoint, which is version 10.
	tree.DeleteVersion(1)

	_, val := tree.GetVersioned(key, 2)
	require.NotEmpty(val)
	require.Equal([]byte("val1"), val)
}

func TestVersionedCheckpointsSpecialCase2(t *testing.T) {
	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("U"), []byte("XamDUtiJ"))
	tree.Set([]byte("A"), []byte("UkZBuYIU"))
	tree.Set([]byte("H"), []byte("7a9En4uw"))
	tree.Set([]byte("V"), []byte("5HXU3pSI"))
	tree.SaveVersion()

	tree.Set([]byte("U"), []byte("Replaced"))
	tree.Set([]byte("A"), []byte("Replaced"))
	tree.SaveVersion()

	tree.Set([]byte("X"), []byte("New"))
	tree.SaveVersion()

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)
}

func TestVersionedCheckpointsSpecialCase3(t *testing.T) {
	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("n"), []byte("2wUCUs8q"))
	tree.Set([]byte("l"), []byte("WQ7mvMbc"))
	tree.SaveVersion()

	tree.Set([]byte("N"), []byte("ved29IqU"))
	tree.Set([]byte("v"), []byte("01jquVXU"))
	tree.SaveVersion()

	tree.Set([]byte("l"), []byte("bhIpltPM"))
	tree.Set([]byte("B"), []byte("rj97IKZh"))
	tree.SaveVersion()

	tree.DeleteVersion(2)

	tree.GetVersioned([]byte("m"), 1)
}

func TestVersionedCheckpointsSpecialCase4(t *testing.T) {
	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("U"), []byte("XamDUtiJ"))
	tree.Set([]byte("A"), []byte("UkZBuYIU"))
	tree.Set([]byte("H"), []byte("7a9En4uw"))
	tree.Set([]byte("V"), []byte("5HXU3pSI"))
	tree.SaveVersion()

	tree.Remove([]byte("U"))
	tree.Remove([]byte("A"))
	tree.SaveVersion()

	tree.Set([]byte("X"), []byte("New"))
	tree.SaveVersion()

	_, val := tree.GetVersioned([]byte("A"), 2)
	require.Nil(t, val)

	_, val = tree.GetVersioned([]byte("A"), 1)
	require.NotEmpty(t, val)

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)

	_, val = tree.GetVersioned([]byte("A"), 2)
	require.Nil(t, val)

	_, val = tree.GetVersioned([]byte("A"), 1)
	require.Nil(t, val)
}

func TestVersionedCheckpointsSpecialCase5(t *testing.T) {
	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("R"), []byte("ygZlIzeW"))
	tree.SaveVersion()

	tree.Set([]byte("j"), []byte("ZgmCWyo2"))
	tree.SaveVersion()

	tree.Set([]byte("R"), []byte("vQDaoz6Z"))
	tree.SaveVersion()

	tree.DeleteVersion(1)

	tree.GetVersioned([]byte("R"), 2)
}

func TestVersionedCheckpointsSpecialCase6(t *testing.T) {
	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("Y"), []byte("MW79JQeV"))
	tree.Set([]byte("7"), []byte("Kp0ToUJB"))
	tree.Set([]byte("Z"), []byte("I26B1jPG"))
	tree.Set([]byte("6"), []byte("ZG0iXq3h"))
	tree.Set([]byte("2"), []byte("WOR27LdW"))
	tree.Set([]byte("4"), []byte("MKMvc6cn"))
	tree.SaveVersion()

	tree.Set([]byte("1"), []byte("208dOu40"))
	tree.Set([]byte("G"), []byte("7isI9OQH"))
	tree.Set([]byte("8"), []byte("zMC1YwpH"))
	tree.SaveVersion()

	tree.Set([]byte("7"), []byte("bn62vWbq"))
	tree.Set([]byte("5"), []byte("wZuLGDkZ"))
	tree.SaveVersion()

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)

	tree.GetVersioned([]byte("Y"), 1)
	tree.GetVersioned([]byte("7"), 1)
	tree.GetVersioned([]byte("Z"), 1)
	tree.GetVersioned([]byte("6"), 1)
	tree.GetVersioned([]byte("s"), 1)
	tree.GetVersioned([]byte("2"), 1)
	tree.GetVersioned([]byte("4"), 1)
}

func TestVersionedCheckpointsSpecialCase7(t *testing.T) {
	tree := NewVersionedTree(db.NewMemDB(), 100)

	tree.Set([]byte("n"), []byte("OtqD3nyn"))
	tree.Set([]byte("W"), []byte("kMdhJjF5"))
	tree.Set([]byte("A"), []byte("BM3BnrIb"))
	tree.Set([]byte("I"), []byte("QvtCH970"))
	tree.Set([]byte("L"), []byte("txKgOTqD"))
	tree.Set([]byte("Y"), []byte("NAl7PC5L"))
	tree.SaveVersion()

	tree.Set([]byte("7"), []byte("qWcEAlyX"))
	tree.SaveVersion()

	tree.Set([]byte("M"), []byte("HdQwzA64"))
	tree.Set([]byte("3"), []byte("2Naa77fo"))
	tree.Set([]byte("A"), []byte("SRuwKOTm"))
	tree.Set([]byte("I"), []byte("oMX4aAOy"))
	tree.Set([]byte("4"), []byte("dKfvbEOc"))
	tree.SaveVersion()

	tree.Set([]byte("D"), []byte("3U4QbXCC"))
	tree.Set([]byte("B"), []byte("FxExhiDq"))
	tree.SaveVersion()

	tree.Set([]byte("A"), []byte("tWQgbFCY"))
	tree.SaveVersion()

	tree.DeleteVersion(4)

	tree.GetVersioned([]byte("A"), 3)
}

func TestVersionedTreeEfficiency(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 0)
	versions := 20
	keysPerVersion := 100
	keysAddedPerVersion := map[int]int{}

	keysAdded := 0
	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			// Keys of size one are likely to be overwritten.
			tree.Set([]byte(cmn.RandStr(1)), []byte(cmn.RandStr(8)))
		}
		sizeBefore := len(tree.ndb.nodes())
		tree.SaveVersion()
		sizeAfter := len(tree.ndb.nodes())
		change := sizeAfter - sizeBefore
		keysAddedPerVersion[i] = change
		keysAdded += change
	}

	keysDeleted := 0
	for i := 1; i < versions; i++ {
		sizeBefore := len(tree.ndb.nodes())
		tree.DeleteVersion(int64(i))
		sizeAfter := len(tree.ndb.nodes())

		change := sizeBefore - sizeAfter
		keysDeleted += change

		require.InDelta(change, keysAddedPerVersion[i], float64(keysPerVersion)/5)
	}
	require.Equal(keysAdded-tree.nodeSize(), keysDeleted)
}

func TestVersionedTreeProofs(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("k1"), []byte("v1"))
	tree.Set([]byte("k2"), []byte("v1"))
	tree.Set([]byte("k3"), []byte("v1"))
	tree.SaveVersion()

	root1 := tree.Hash()

	tree.Set([]byte("k2"), []byte("v2"))
	tree.Set([]byte("k4"), []byte("v2"))
	tree.SaveVersion()

	root2 := tree.Hash()
	require.NotEqual(root1, root2)

	tree.Remove([]byte("k2"))
	tree.SaveVersion()

	root3 := tree.Hash()
	require.NotEqual(root2, root3)

	val, proof, err := tree.GetVersionedWithProof([]byte("k2"), 1)
	require.NoError(err)
	require.EqualValues(val, []byte("v1"))
	require.EqualValues(1, proof.(*KeyExistsProof).Version)
	require.NoError(proof.Verify([]byte("k2"), val, root1))

	val, proof, err = tree.GetVersionedWithProof([]byte("k4"), 1)
	require.NoError(err)
	require.Nil(val)
	require.NoError(proof.Verify([]byte("k4"), nil, root1))
	require.Error(proof.Verify([]byte("k4"), val, root2))

	val, proof, err = tree.GetVersionedWithProof([]byte("k2"), 2)
	require.NoError(err)
	require.EqualValues(val, []byte("v2"))
	require.NoError(proof.Verify([]byte("k2"), val, root2))
	require.Error(proof.Verify([]byte("k2"), val, root1))

	val, proof, err = tree.GetVersionedWithProof([]byte("k1"), 2)
	require.NoError(err)
	require.EqualValues(val, []byte("v1"))
	require.NoError(proof.Verify([]byte("k1"), val, root2))

	val, proof, err = tree.GetVersionedWithProof([]byte("k2"), 3)
	require.NoError(err)
	require.Nil(val)
	require.NoError(proof.Verify([]byte("k2"), nil, root3))
	require.Error(proof.Verify([]byte("k2"), nil, root1))
	require.Error(proof.Verify([]byte("k2"), nil, root2))
}

func TestVersionedTreeHash(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 0)

	require.Nil(tree.Hash())
	tree.Set([]byte("I"), []byte("D"))
	require.Nil(tree.Hash())

	hash1, _, _ := tree.SaveVersion()

	tree.Set([]byte("I"), []byte("F"))
	require.EqualValues(hash1, tree.Hash())

	hash2, _, _ := tree.SaveVersion()

	val, proof, err := tree.GetVersionedWithProof([]byte("I"), 2)
	require.NoError(err)
	require.EqualValues(val, []byte("F"))
	require.NoError(proof.Verify([]byte("I"), val, hash2))
}

func TestNilValueSemantics(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(db.NewMemDB(), 0)

	require.Panics(func() {
		tree.Set([]byte("k"), nil)
	})
}

func TestCopyValueSemantics(t *testing.T) {
	require := require.New(t)

	tree := NewVersionedTree(db.NewMemDB(), 0)

	val := []byte("v1")

	tree.Set([]byte("k"), val)
	_, v := tree.Get([]byte("k"))
	require.Equal([]byte("v1"), v)

	val[1] = '2'

	_, val = tree.Get([]byte("k"))
	require.Equal([]byte("v2"), val)
}

func TestRollback(t *testing.T) {
	require := require.New(t)

	tree := NewVersionedTree(db.NewMemDB(), 0)

	tree.Set([]byte("k"), []byte("v"))
	tree.SaveVersion()

	tree.Set([]byte("r"), []byte("v"))
	tree.Set([]byte("s"), []byte("v"))

	tree.Rollback()

	tree.Set([]byte("t"), []byte("v"))

	tree.SaveVersion()

	require.Equal(2, tree.Size())

	_, val := tree.Get([]byte("r"))
	require.Nil(val)

	_, val = tree.Get([]byte("s"))
	require.Nil(val)

	_, val = tree.Get([]byte("t"))
	require.Equal([]byte("v"), val)
}

//////////////////////////// BENCHMARKS ///////////////////////////////////////

func BenchmarkTreeLoadAndDelete(b *testing.B) {
	numVersions := 5000
	numKeysPerVersion := 10

	d, err := db.NewGoLevelDB("bench", ".")
	if err != nil {
		panic(err)
	}
	defer d.Close()
	defer os.RemoveAll("./bench.db")

	tree := NewVersionedTree(d, 0)
	for v := 1; v < numVersions; v++ {
		for i := 0; i < numKeysPerVersion; i++ {
			tree.Set([]byte(cmn.RandStr(16)), cmn.RandBytes(32))
		}
		tree.SaveVersion()
	}

	b.Run("LoadAndDelete", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			tree = NewVersionedTree(d, 0)
			runtime.GC()
			b.StartTimer()

			// Load the tree from disk.
			tree.Load()

			// Delete about 10% of the versions randomly.
			// The trade-off is usually between load efficiency and delete
			// efficiency, which is why we do both in this benchmark.
			// If we can load quickly into a data-structure that allows for
			// efficient deletes, we are golden.
			for v := 0; v < numVersions/10; v++ {
				version := (cmn.RandInt() % numVersions) + 1
				tree.DeleteVersion(int64(version))
			}
		}
	})
}
