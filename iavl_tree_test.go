package iavl

import (
	"bytes"
	"flag"
	"os"
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

func TestVersionedRandomTree(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(100, db.NewMemDB())
	versions := 50
	keysPerVersion := 30

	// Create a tree of size 1000 with 100 versions.
	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			k := []byte(cmn.RandStr(8))
			v := []byte(cmn.RandStr(8))
			tree.Set(k, v)
		}
		tree.SaveVersion(uint64(i))
	}
	require.Equal(versions, len(tree.ndb.roots()), "wrong number of roots")
	require.Equal(versions*keysPerVersion, len(tree.ndb.leafNodes()), "wrong number of nodes")

	// Before deleting old versions, we should have equal or more nodes in the
	// db than in the current tree version.
	require.True(len(tree.ndb.nodes()) >= tree.nodeSize())

	for i := 1; i < versions; i++ {
		tree.DeleteVersion(uint64(i))
	}

	require.Len(tree.versions, 1, "tree must have one version left")
	require.Equal(tree.versions[uint64(versions)].root, tree.root)

	// After cleaning up all previous versions, we should have as many nodes
	// in the db as in the current tree version.
	require.Len(tree.ndb.leafNodes(), tree.Size())

	require.Equal(tree.nodeSize(), len(tree.ndb.nodes()))
}

func TestVersionedRandomTreeSmallKeys(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(100, db.NewMemDB())
	versions := 20
	keysPerVersion := 50

	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			// Keys of size one are likely to be overwritten.
			tree.Set([]byte(cmn.RandStr(1)), []byte(cmn.RandStr(8)))
		}
		tree.SaveVersion(uint64(i))
	}

	for i := 1; i < versions; i++ {
		tree.DeleteVersion(uint64(i))
	}

	// After cleaning up all previous versions, we should have as many nodes
	// in the db as in the current tree version.
	require.Len(tree.ndb.leafNodes(), tree.Size())
	require.Len(tree.ndb.nodes(), tree.nodeSize())

	// Try getting random keys.
	for i := 0; i < keysPerVersion; i++ {
		_, val, exists := tree.Get([]byte(cmn.RandStr(1)))
		require.True(exists)
		require.NotEmpty(val)
	}
}

func TestVersionedTreeSpecial1(t *testing.T) {
	tree := NewVersionedTree(100, db.NewMemDB())

	tree.Set([]byte("C"), []byte("so43QQFN"))
	tree.SaveVersion(1)

	tree.Set([]byte("A"), []byte("ut7sTTAO"))
	tree.SaveVersion(2)

	tree.Set([]byte("X"), []byte("AoWWC1kN"))
	tree.SaveVersion(3)

	tree.Set([]byte("T"), []byte("MhkWjkVy"))
	tree.SaveVersion(4)

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)
	tree.DeleteVersion(3)

	require.Len(t, tree.ndb.nodes(), tree.nodeSize())
}

func TestVersionedRandomTreeSpecial2(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(100, db.NewMemDB())

	tree.Set([]byte("OFMe2Yvm"), []byte("ez2OtQtE"))
	tree.Set([]byte("WEN4iN7Y"), []byte("kQNyUalI"))
	tree.SaveVersion(1)

	tree.Set([]byte("1yY3pXHr"), []byte("udYznpII"))
	tree.Set([]byte("7OSHNE7k"), []byte("ff181M2d"))
	tree.SaveVersion(2)

	tree.DeleteVersion(1)
	require.Len(tree.ndb.nodes(), tree.nodeSize())
}

func TestVersionedTree(t *testing.T) {
	require := require.New(t)

	var d db.DB
	var err error

	if testLevelDB {
		d, err = db.NewGoLevelDB("test", ".")
		require.NoError(err)
		defer d.Close()
		defer os.RemoveAll("./test.db")
	} else {
		d = db.NewMemDB()
	}

	tree := NewVersionedTree(100, d)

	// We start with zero keys in the databse.
	require.Equal(0, tree.ndb.size())

	// version 0

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))

	// Still zero keys, since we haven't written them.
	require.Len(tree.ndb.leafNodes(), 0)

	// Saving with version zero is an error.
	_, err = tree.SaveVersion(0)
	require.Error(err)

	// Now let's write the keys to storage.
	hash1, err := tree.SaveVersion(1)
	require.NoError(err)

	// Saving twice with the same version is an error.
	_, err = tree.SaveVersion(1)
	require.Error(err)

	// -----1-----
	// key1 = val0
	// key2 = val0
	// -----------

	nodes1 := tree.ndb.leafNodes()
	require.Len(nodes1, 2, "db should have a size of 2")

	// version  1

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.Set([]byte("key3"), []byte("val1"))
	require.Len(tree.ndb.leafNodes(), len(nodes1))

	hash2, err := tree.SaveVersion(2)
	require.NoError(err)
	require.False(bytes.Equal(hash1, hash2))

	// Recreate a new tree and load it, to make sure it works in this
	// scenario.
	tree = NewVersionedTree(100, d)
	require.NoError(tree.Load())

	require.Len(tree.versions, 2, "wrong number of versions")

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

	hash3, _ := tree.SaveVersion(3)

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

	hash4, _ := tree.SaveVersion(4)
	require.EqualValues(hash3, hash4)
	require.NotNil(hash4)

	tree = NewVersionedTree(100, d)
	require.NoError(tree.Load())

	// ------------
	// DB UNCHANGED
	// ------------

	nodes4 := tree.ndb.leafNodes()
	require.Len(nodes4, len(nodes3), "db should not have changed in size")

	tree.Set([]byte("key1"), []byte("val0"))

	// "key2"
	_, _, exists := tree.GetVersioned([]byte("key2"), 0)
	require.False(exists)

	_, val, _ := tree.GetVersioned([]byte("key2"), 1)
	require.Equal("val0", string(val))

	_, val, _ = tree.GetVersioned([]byte("key2"), 2)
	require.Equal("val1", string(val))

	_, val, _ = tree.Get([]byte("key2"))
	require.Equal("val2", string(val))

	// "key1"
	_, val, _ = tree.GetVersioned([]byte("key1"), 1)
	require.Equal("val0", string(val))

	_, val, _ = tree.GetVersioned([]byte("key1"), 2)
	require.Equal("val1", string(val))

	_, val, exists = tree.GetVersioned([]byte("key1"), 3)
	require.Nil(val)
	require.False(exists)

	_, val, exists = tree.GetVersioned([]byte("key1"), 4)
	require.Nil(val)
	require.False(exists)

	_, val, _ = tree.Get([]byte("key1"))
	require.Equal("val0", string(val))

	// "key3"
	_, val, exists = tree.GetVersioned([]byte("key3"), 0)
	require.Nil(val)
	require.False(exists)

	_, val, _ = tree.GetVersioned([]byte("key3"), 2)
	require.Equal("val1", string(val))

	_, val, _ = tree.GetVersioned([]byte("key3"), 3)
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
	require.True(len(nodes5) < len(nodes4), "db should have shrunk after delete")

	_, val, exists = tree.GetVersioned([]byte("key2"), 2)
	require.False(exists)
	require.Nil(val)

	_, val, exists = tree.GetVersioned([]byte("key3"), 2)
	require.False(exists)
	require.Nil(val)

	// But they should still exist in the latest version.

	_, val, _ = tree.Get([]byte("key2"))
	require.Equal("val2", string(val))

	_, val, _ = tree.Get([]byte("key3"))
	require.Equal("val1", string(val))

	// Version 1 should still be available.

	_, val, _ = tree.GetVersioned([]byte("key1"), 1)
	require.Equal("val0", string(val))

	_, val, _ = tree.GetVersioned([]byte("key2"), 1)
	require.Equal("val0", string(val))
}

func TestVersionedTreeVersionDeletingEfficiency(t *testing.T) {
	tree := NewVersionedTree(0, db.NewMemDB())

	tree.Set([]byte("key0"), []byte("val0"))
	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))
	tree.SaveVersion(1)

	require.Len(t, tree.ndb.leafNodes(), 3)

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.Set([]byte("key3"), []byte("val1"))
	tree.SaveVersion(2)

	require.Len(t, tree.ndb.leafNodes(), 6)

	tree.Set([]byte("key0"), []byte("val2"))
	tree.Remove([]byte("key1"))
	tree.Set([]byte("key2"), []byte("val2"))
	tree.SaveVersion(3)

	require.Len(t, tree.ndb.leafNodes(), 8)

	tree.DeleteVersion(2)

	require.Len(t, tree.ndb.leafNodes(), 6)

	tree.DeleteVersion(1)

	require.Len(t, tree.ndb.leafNodes(), 3)
}

func TestVersionedTreeOrphanDeleting(t *testing.T) {
	tree := NewVersionedTree(0, db.NewMemDB())

	tree.Set([]byte("key0"), []byte("val0"))
	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))
	tree.SaveVersion(1)

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.Set([]byte("key3"), []byte("val1"))
	tree.SaveVersion(2)

	tree.Set([]byte("key0"), []byte("val2"))
	tree.Remove([]byte("key1"))
	tree.Set([]byte("key2"), []byte("val2"))
	tree.SaveVersion(3)
	tree.DeleteVersion(2)

	_, val, _ := tree.Get([]byte("key0"))
	require.Equal(t, val, []byte("val2"))

	_, val, exists := tree.Get([]byte("key1"))
	require.Empty(t, val)
	require.False(t, exists)

	_, val, _ = tree.Get([]byte("key2"))
	require.Equal(t, val, []byte("val2"))

	_, val, _ = tree.Get([]byte("key3"))
	require.Equal(t, val, []byte("val1"))

	tree.DeleteVersion(1)

	require.Len(t, tree.ndb.leafNodes(), 3)
}

func TestVersionedTreeSpecialCase(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(100, db.NewMemDB())

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))
	tree.SaveVersion(1)

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.SaveVersion(2)

	tree.Set([]byte("key2"), []byte("val2"))
	tree.SaveVersion(4)

	tree.DeleteVersion(2)

	_, val, _ := tree.GetVersioned([]byte("key2"), 1)
	require.Equal("val0", string(val))
}

func TestVersionedTreeSpecialCase2(t *testing.T) {
	require := require.New(t)
	d := db.NewMemDB()

	tree := NewVersionedTree(100, d)

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))
	tree.SaveVersion(1)

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.SaveVersion(2)

	tree.Set([]byte("key2"), []byte("val2"))
	tree.SaveVersion(4)

	tree = NewVersionedTree(100, d)
	require.NoError(tree.Load())

	require.NoError(tree.DeleteVersion(2))

	_, val, _ := tree.GetVersioned([]byte("key2"), 1)
	require.Equal("val0", string(val))
}

func TestVersionedTreeSpecialCase3(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(0, db.NewMemDB())

	tree.Set([]byte("m"), []byte("liWT0U6G"))
	tree.Set([]byte("G"), []byte("7PxRXwUA"))
	tree.SaveVersion(1)

	tree.Set([]byte("7"), []byte("XRLXgf8C"))
	tree.SaveVersion(2)

	tree.Set([]byte("r"), []byte("bBEmIXBU"))
	tree.SaveVersion(3)

	tree.Set([]byte("i"), []byte("kkIS35te"))
	tree.SaveVersion(4)

	tree.Set([]byte("k"), []byte("CpEnpzKJ"))
	tree.SaveVersion(5)

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)
	tree.DeleteVersion(3)
	tree.DeleteVersion(4)

	require.Equal(tree.nodeSize(), len(tree.ndb.nodes()))
}

func TestVersionedTreeSaveAndLoad(t *testing.T) {
	require := require.New(t)
	d := db.NewMemDB()
	tree := NewVersionedTree(0, d)

	tree.Set([]byte("C"), []byte("so43QQFN"))
	tree.SaveVersion(1)

	tree.Set([]byte("A"), []byte("ut7sTTAO"))
	tree.SaveVersion(2)

	tree.Set([]byte("X"), []byte("AoWWC1kN"))
	tree.SaveVersion(3)

	// Reload the tree, to test that roots and orphans are properly loaded.
	tree = NewVersionedTree(0, d)
	tree.Load()

	tree.Set([]byte("T"), []byte("MhkWjkVy"))
	tree.SaveVersion(4)

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)
	tree.DeleteVersion(3)

	require.Equal(4, tree.Size())
	require.Len(tree.ndb.nodes(), tree.nodeSize())
}

func TestVersionedTreeErrors(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(100, db.NewMemDB())

	// Can't save with empty tree.
	_, err := tree.SaveVersion(1)
	require.Error(err)

	// Can't delete non-existent versions.
	require.Error(tree.DeleteVersion(1))
	require.Error(tree.DeleteVersion(99))

	tree.Set([]byte("key"), []byte("val"))

	// `0` is an invalid version number.
	_, err = tree.SaveVersion(0)
	require.Error(err)

	// Saving version `1` is ok.
	_, err = tree.SaveVersion(1)
	require.NoError(err)

	// Can't delete current version.
	require.Error(tree.DeleteVersion(1))
}

func TestVersionedCheckpoints(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(100, db.NewMemDB())
	versions := 50
	keysPerVersion := 10
	versionsPerCheckpoint := 5
	keys := map[uint64]([][]byte){}

	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			k := []byte(cmn.RandStr(1))
			v := []byte(cmn.RandStr(8))
			keys[uint64(i)] = append(keys[uint64(i)], k)
			tree.Set(k, v)
		}
		tree.SaveVersion(uint64(i))
	}

	for i := 1; i <= versions; i++ {
		if i%versionsPerCheckpoint != 0 {
			tree.DeleteVersion(uint64(i))
		}
	}

	// Make sure all keys exist at least once.
	for _, ks := range keys {
		for _, k := range ks {
			_, val, exists := tree.Get(k)
			require.True(exists)
			require.NotEmpty(val)
		}
	}

	// Make sure all keys from deleted versions aren't present.
	for i := 1; i <= versions; i++ {
		if i%versionsPerCheckpoint != 0 {
			for _, k := range keys[uint64(i)] {
				_, val, exists := tree.GetVersioned(k, uint64(i))
				require.False(exists)
				require.Empty(val)
			}
		}
	}

	// Make sure all keys exist at all checkpoints.
	for i := 1; i <= versions; i++ {
		for _, k := range keys[uint64(i)] {
			if i%versionsPerCheckpoint == 0 {
				_, val, exists := tree.GetVersioned(k, uint64(i))
				require.True(exists, "key %s should exist at version %d", k, i)
				require.NotEmpty(val)
			}
		}
	}
}

func TestVersionedCheckpointsSpecialCase(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(0, db.NewMemDB())
	key := []byte("k")

	tree.Set(key, []byte("val1"))

	tree.SaveVersion(1)
	// ...
	tree.SaveVersion(10)
	// ...
	tree.SaveVersion(19)
	// ...
	// This orphans "k" at version 1.
	tree.Set(key, []byte("val2"))
	tree.SaveVersion(20)

	// When version 1 is deleted, the orphans should move to the next
	// checkpoint, which is version 10.
	tree.DeleteVersion(1)

	_, val, exists := tree.GetVersioned(key, 10)
	require.True(exists)
	require.Equal([]byte("val1"), val)
}

func TestVersionedCheckpointsSpecialCase2(t *testing.T) {
	tree := NewVersionedTree(0, db.NewMemDB())

	tree.Set([]byte("U"), []byte("XamDUtiJ"))
	tree.Set([]byte("A"), []byte("UkZBuYIU"))
	tree.Set([]byte("H"), []byte("7a9En4uw"))
	tree.Set([]byte("V"), []byte("5HXU3pSI"))
	tree.SaveVersion(1)

	tree.Set([]byte("U"), []byte("Replaced"))
	tree.Set([]byte("A"), []byte("Replaced"))
	tree.SaveVersion(2)

	tree.Set([]byte("X"), []byte("New"))
	tree.SaveVersion(3)

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)
}

func TestVersionedCheckpointsSpecialCase3(t *testing.T) {
	tree := NewVersionedTree(0, db.NewMemDB())

	tree.Set([]byte("n"), []byte("2wUCUs8q"))
	tree.Set([]byte("l"), []byte("WQ7mvMbc"))
	tree.SaveVersion(2)

	tree.Set([]byte("N"), []byte("ved29IqU"))
	tree.Set([]byte("v"), []byte("01jquVXU"))
	tree.SaveVersion(5)

	tree.Set([]byte("l"), []byte("bhIpltPM"))
	tree.Set([]byte("B"), []byte("rj97IKZh"))
	tree.SaveVersion(6)

	tree.DeleteVersion(5)

	tree.GetVersioned([]byte("m"), 2)
}

func TestVersionedCheckpointsSpecialCase4(t *testing.T) {
	tree := NewVersionedTree(0, db.NewMemDB())

	tree.Set([]byte("U"), []byte("XamDUtiJ"))
	tree.Set([]byte("A"), []byte("UkZBuYIU"))
	tree.Set([]byte("H"), []byte("7a9En4uw"))
	tree.Set([]byte("V"), []byte("5HXU3pSI"))
	tree.SaveVersion(1)

	tree.Remove([]byte("U"))
	tree.Remove([]byte("A"))
	tree.SaveVersion(2)

	tree.Set([]byte("X"), []byte("New"))
	tree.SaveVersion(3)

	_, _, exists := tree.GetVersioned([]byte("A"), 2)
	require.False(t, exists)

	_, _, exists = tree.GetVersioned([]byte("A"), 1)
	require.True(t, exists)

	tree.DeleteVersion(1)
	tree.DeleteVersion(2)

	_, _, exists = tree.GetVersioned([]byte("A"), 2)
	require.False(t, exists)

	_, _, exists = tree.GetVersioned([]byte("A"), 1)
	require.False(t, exists)
}

func TestVersionedCheckpointsSpecialCase5(t *testing.T) {
	tree := NewVersionedTree(0, db.NewMemDB())

	tree.Set([]byte("R"), []byte("ygZlIzeW"))
	tree.SaveVersion(1)

	tree.Set([]byte("j"), []byte("ZgmCWyo2"))
	tree.SaveVersion(2)

	tree.Set([]byte("R"), []byte("vQDaoz6Z"))
	tree.SaveVersion(3)

	tree.DeleteVersion(1)

	tree.GetVersioned([]byte("R"), 2)
}

func TestVersionedCheckpointsSpecialCase6(t *testing.T) {
	tree := NewVersionedTree(0, db.NewMemDB())

	tree.Set([]byte("Y"), []byte("MW79JQeV"))
	tree.Set([]byte("7"), []byte("Kp0ToUJB"))
	tree.Set([]byte("Z"), []byte("I26B1jPG"))
	tree.Set([]byte("6"), []byte("ZG0iXq3h"))
	tree.Set([]byte("2"), []byte("WOR27LdW"))
	tree.Set([]byte("4"), []byte("MKMvc6cn"))
	tree.SaveVersion(1)

	tree.Set([]byte("1"), []byte("208dOu40"))
	tree.Set([]byte("G"), []byte("7isI9OQH"))
	tree.Set([]byte("8"), []byte("zMC1YwpH"))
	tree.SaveVersion(2)

	tree.Set([]byte("7"), []byte("bn62vWbq"))
	tree.Set([]byte("5"), []byte("wZuLGDkZ"))
	tree.SaveVersion(3)

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
	tree := NewVersionedTree(100, db.NewMemDB())

	tree.Set([]byte("n"), []byte("OtqD3nyn"))
	tree.Set([]byte("W"), []byte("kMdhJjF5"))
	tree.Set([]byte("A"), []byte("BM3BnrIb"))
	tree.Set([]byte("I"), []byte("QvtCH970"))
	tree.Set([]byte("L"), []byte("txKgOTqD"))
	tree.Set([]byte("Y"), []byte("NAl7PC5L"))
	tree.SaveVersion(1)

	tree.Set([]byte("7"), []byte("qWcEAlyX"))
	tree.SaveVersion(2)

	tree.Set([]byte("M"), []byte("HdQwzA64"))
	tree.Set([]byte("3"), []byte("2Naa77fo"))
	tree.Set([]byte("A"), []byte("SRuwKOTm"))
	tree.Set([]byte("I"), []byte("oMX4aAOy"))
	tree.Set([]byte("4"), []byte("dKfvbEOc"))
	tree.SaveVersion(3)

	tree.Set([]byte("D"), []byte("3U4QbXCC"))
	tree.Set([]byte("B"), []byte("FxExhiDq"))
	tree.SaveVersion(5)

	tree.Set([]byte("A"), []byte("tWQgbFCY"))
	tree.SaveVersion(6)

	tree.DeleteVersion(5)

	tree.GetVersioned([]byte("A"), 3)
}

func TestVersionedTreeEfficiency(t *testing.T) {
	require := require.New(t)
	tree := NewVersionedTree(0, db.NewMemDB())
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
		tree.SaveVersion(uint64(i))
		sizeAfter := len(tree.ndb.nodes())
		change := sizeAfter - sizeBefore
		keysAddedPerVersion[i] = change
		keysAdded += change
	}

	keysDeleted := 0
	for i := 1; i < versions; i++ {
		sizeBefore := len(tree.ndb.nodes())
		tree.DeleteVersion(uint64(i))
		sizeAfter := len(tree.ndb.nodes())

		change := sizeBefore - sizeAfter
		keysDeleted += change

		require.InDelta(change, keysAddedPerVersion[i], float64(keysPerVersion)/5)
	}
	require.Equal(keysAdded-tree.nodeSize(), keysDeleted)
}
