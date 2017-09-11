package iavl

import (
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tmlibs/db"

	cmn "github.com/tendermint/tmlibs/common"
)

var testLevelDB bool

func init() {
	flag.BoolVar(&testLevelDB, "test.leveldb", false, "test leveldb backend")
	flag.Parse()
}

func TestVersionedRandomTree(t *testing.T) {
	require := require.New(t)
	tree := NewIAVLVersionedTree(100, db.NewMemDB())
	versions := 100
	keysPerVersion := 30

	// Create a tree of size 1000 with 100 versions.
	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			tree.Set([]byte(cmn.RandStr(8)), []byte(cmn.RandStr(8)))
		}
		tree.SaveVersion(uint64(i))
	}
	require.Equal(versions, len(tree.ndb.roots()), "wrong number of roots")
	require.Equal(versions*keysPerVersion, len(tree.ndb.leafNodes()), "wrong number of nodes")

	// Before deleting old versions, we should have equal or more nodes in the
	// db than in the current tree version.
	require.True(len(tree.ndb.nodes()) >= tree.nodeSize())

	// XXX: Since the HEAD was not persisted, it still depends on a previous
	// copy, which is a problem when it is deleted.

	for i := 1; i < versions; i++ {
		tree.ReleaseVersion(uint64(i))
	}

	require.Len(tree.versions, 1, "tree must have one version left")
	require.Equal(tree.versions[uint64(versions)].root, tree.root)

	// After cleaning up all previous versions, we should have as many nodes
	// in the db as in the current tree version.
	require.Len(tree.ndb.leafNodes(), tree.Size())
	require.Len(tree.ndb.nodes(), tree.nodeSize())
}

func TestVersionedRandomTreeSpecial1(t *testing.T) {
	require := require.New(t)
	tree := NewIAVLVersionedTree(100, db.NewMemDB())

	tree.Set([]byte("C"), []byte("so43QQFN"))
	tree.SaveVersion(1)

	tree.Set([]byte("A"), []byte("ut7sTTAO"))
	tree.SaveVersion(2)

	tree.Set([]byte("X"), []byte("AoWWC1kN"))
	tree.SaveVersion(3)

	tree.Set([]byte("T"), []byte("MhkWjkVy"))
	tree.SaveVersion(4)

	tree.ReleaseVersion(1)
	tree.ReleaseVersion(2)
	tree.ReleaseVersion(3)

	require.Len(tree.ndb.nodes(), tree.nodeSize())
}

func TestVersionedRandomTreeSpecial2(t *testing.T) {
	require := require.New(t)
	tree := NewIAVLVersionedTree(100, db.NewMemDB())

	tree.Set([]byte("OFMe2Yvm"), []byte("ez2OtQtE"))
	tree.Set([]byte("WEN4iN7Y"), []byte("kQNyUalI"))
	tree.SaveVersion(1)

	tree.Set([]byte("1yY3pXHr"), []byte("udYznpII"))
	tree.Set([]byte("7OSHNE7k"), []byte("ff181M2d"))
	tree.SaveVersion(2)

	// XXX: The root of Version 1 is being marked as an orphan, but is
	// still in use by the Version 2 tree. This is the problem.

	tree.ReleaseVersion(1)
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

	tree := NewIAVLVersionedTree(100, d)

	// We start with zero keys in the databse.
	require.Equal(0, tree.ndb.size())

	// version 0

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))

	// Still zero keys, since we haven't written them.
	require.Len(tree.ndb.leafNodes(), 0)

	err = tree.SaveVersion(1)
	require.NoError(err)

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

	err = tree.SaveVersion(2)
	require.NoError(err)

	// Recreate a new tree and load it, to make sure it works in this
	// scenario.
	tree = NewIAVLVersionedTree(100, d)
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
	require.Len(nodes2, 5, "db should have grown in size\n%s", tree.ndb.String())
	require.Len(tree.ndb.orphans(), 3, "db should have three orphans\n%s", tree.ndb.String())

	// Create two more orphans.
	tree.Remove([]byte("key1"))
	tree.Set([]byte("key2"), []byte("val2"))

	tree.SaveVersion(3)

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
	require.Len(nodes3, 6, "wrong number of nodes\n%s", tree.ndb.String())
	require.Len(tree.ndb.orphans(), 6, "wrong number of orphans\n%s", tree.ndb.String())

	tree.SaveVersion(4)
	tree = NewIAVLVersionedTree(100, d)
	require.NoError(tree.Load())

	// ------------
	// DB UNCHANGED
	// ------------

	nodes4 := tree.ndb.leafNodes()
	require.Len(nodes4, len(nodes3), "db should not have changed in size\n%s", tree.ndb.String())

	tree.Set([]byte("key1"), []byte("val0"))

	// "key2"
	_, _, exists := tree.GetVersion([]byte("key2"), 0)
	require.False(exists)

	_, val, _ := tree.GetVersion([]byte("key2"), 1)
	require.Equal("val0", string(val))

	_, val, _ = tree.GetVersion([]byte("key2"), 2)
	require.Equal("val1", string(val))

	_, val, _ = tree.Get([]byte("key2"))
	require.Equal("val2", string(val))

	// "key1"
	_, val, _ = tree.GetVersion([]byte("key1"), 1)
	require.Equal("val0", string(val))

	_, val, _ = tree.GetVersion([]byte("key1"), 2)
	require.Equal("val1", string(val))

	_, val, exists = tree.GetVersion([]byte("key1"), 3)
	require.Nil(val)
	require.False(exists)

	_, val, exists = tree.GetVersion([]byte("key1"), 4)
	require.Nil(val)
	require.False(exists)

	_, val, _ = tree.Get([]byte("key1"))
	require.Equal("val0", string(val))

	// "key3"
	_, val, exists = tree.GetVersion([]byte("key3"), 0)
	require.Nil(val)
	require.False(exists)

	_, val, _ = tree.GetVersion([]byte("key3"), 2)
	require.Equal("val1", string(val))

	_, val, _ = tree.GetVersion([]byte("key3"), 3)
	require.Equal("val1", string(val))

	// Release a version. After this the keys in that version should not be found.

	before := tree.ndb.String()
	tree.ReleaseVersion(2)

	// -----1-----
	// key1 = val0
	// key2 = val0
	// -----2-----
	// key3 = val1
	// -----3-----
	// key2 = val2
	// -----------

	nodes5 := tree.ndb.leafNodes()
	require.Len(nodes5, 4, "db should have shrunk after release\n%s\nvs.\n%s", before, tree.ndb.String())

	_, val, exists = tree.GetVersion([]byte("key2"), 2)
	require.False(exists)
	require.Nil(val)

	_, val, exists = tree.GetVersion([]byte("key3"), 2)
	require.False(exists)
	require.Nil(val)

	// But they should still exist in the latest version.

	_, val, _ = tree.Get([]byte("key2"))
	require.Equal("val2", string(val))

	_, val, _ = tree.Get([]byte("key3"))
	require.Equal("val1", string(val))

	// Version 1 should still be available.

	_, val, _ = tree.GetVersion([]byte("key1"), 1)
	require.Equal("val0", string(val))

	_, val, _ = tree.GetVersion([]byte("key2"), 1)
	require.Equal("val0", string(val))
}
