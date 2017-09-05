package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tmlibs/db"
)

func TestVersionedTree(t *testing.T) {
	require := require.New(t)
	tree := NewIAVLVersionedTree(100, db.NewMemDB())

	tree.SaveVersion(0)

	// We start with zero keys in the databse.
	require.Equal(0, tree.head.ndb.size())

	// version 0

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))

	// Still zero keys, since we haven't written them.
	require.Len(tree.head.ndb.leafNodes(), 0)

	tree.SaveVersion(1)

	nodes1 := tree.head.ndb.leafNodes()
	require.Len(nodes1, 2, "db should have a size of 2")

	// version  1

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.Set([]byte("key3"), []byte("val1"))
	require.Len(tree.head.ndb.leafNodes(), len(nodes1))

	tree.SaveVersion(2)

	nodes2 := tree.head.ndb.leafNodes()
	require.Len(nodes2, 5, "db should have grown in size")

	// version  2

	tree.Remove([]byte("key1"))
	tree.Set([]byte("key2"), []byte("val2"))

	tree.SaveVersion(3)
	nodes3 := tree.head.ndb.leafNodes()

	// version 3

	tree.SaveVersion(4)

	nodes4 := tree.head.ndb.leafNodes()
	require.Len(nodes4, len(nodes3), "db should not have changed in size\n%s", tree.head.ndb.String())

	// version 4

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

	prev := tree.head.ndb.String()
	tree.ReleaseVersion(2)
	nodes5 := tree.head.ndb.leafNodes()
	require.True(len(nodes5) < len(nodes4), "db should have shrunk in size\n%s\nvs.\n%s", prev, tree.head.ndb.String())

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
}
