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

	// version 0

	tree.Set([]byte("key1"), []byte("val0"))
	tree.Set([]byte("key2"), []byte("val0"))

	tree.SaveVersion(1)

	// version  1

	tree.Set([]byte("key1"), []byte("val1"))
	tree.Set([]byte("key2"), []byte("val1"))
	tree.Set([]byte("key3"), []byte("val1"))

	tree.SaveVersion(2)

	// version  2

	tree.Remove([]byte("key1"))
	tree.Set([]byte("key2"), []byte("val2"))
	tree.Set([]byte("key3"), []byte("val2"))

	tree.SaveVersion(3)

	// version 3

	tree.SaveVersion(4)

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
}
