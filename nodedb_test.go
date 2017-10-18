package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tmlibs/db"
)

func TestNodeDBSaveRoot(t *testing.T) {
	require := require.New(t)
	d := db.NewMemDB()
	ndb := newNodeDB(0, d)

	root1 := NewNode([]byte("root1"), nil)
	root2 := NewNode([]byte("root2"), nil)
	root3 := NewNode([]byte("root3"), nil)
	root4 := NewNode([]byte("root4"), nil)

	root1._hash()
	root2._hash()
	root3._hash()
	root4._hash()

	ndb.SaveRoot(root1, 1)
	ndb.Commit()

	version, _, _ := ndb.getLatestRoot()
	require.Equal(uint64(1), version)

	ndb.SaveRoot(root2, 2)
	ndb.Commit()

	version, _, _ = ndb.getLatestRoot()
	require.Equal(uint64(2), version)

	ndb.SaveRoot(root3, 3)
	ndb.Commit()

	version, _, _ = ndb.getLatestRoot()
	require.Equal(uint64(3), version)

	ndb.SaveRoot(root4, 4)
	ndb.Commit()

	version, _, _ = ndb.getLatestRoot()
	require.Equal(uint64(4), version)

	version, hash, _ := ndb.getLatestRoot()
	require.Equal(uint64(4), version)
	require.Equal(root4._hash(), hash)

	require.Equal(uint64(4), ndb.getLatestVersion())
	require.Equal(4, len(ndb.roots()))

	_, prev, _ := ndb.getRoot(1)
	require.Equal(uint64(0), prev)

	_, _, next := ndb.getRoot(4)
	require.Equal(uint64(0), next)

	_, prev, next = ndb.getRoot(3)
	require.Equal(uint64(2), prev)
	require.Equal(uint64(4), next)

	// Now reload the node database.
	ndb = newNodeDB(0, d)

	require.Equal(uint64(4), ndb.getLatestVersion())
	require.Equal(4, len(ndb.roots()))
}
