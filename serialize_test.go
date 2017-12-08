package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tmlibs/db"
)

// TreeSize is the number of nodes in our test trees
const TreeSize = 10000

func TestSerialize(t *testing.T) {
	require := require.New(t)

	cases := []struct {
		algo     SerializeFunc
		sameHash bool
	}{
		{InOrderSerialize, false},
		{StableSerialize, true},
		{StableSerializeBFS, true},
	}

	for i, tc := range cases {
		tree := makeRandomTree(TreeSize)
		stored := tc.algo(tree, tree.root)
		require.NotNil(stored, "%d", i)
		require.Equal(tree.Size(), len(stored), "%d", i)
		origHash := tree.Hash()
		require.NotNil(origHash)

		empty := NewTree(TreeSize, nil)
		require.Equal(0, empty.Size(), "%d", i)
		Restore(empty, stored)
		require.Equal(tree.Size(), empty.Size(), "%d", i)

		newHash := empty.Hash()
		require.NotNil(newHash)
		if tc.sameHash {
			require.Equal(origHash, newHash, "%d", i)
		} else {
			require.NotEqual(origHash, newHash, "%d", i)
		}
	}
}

// TODO: add some deletes in there as well?
func makeRandomTree(nodes int) *Tree {
	tree := NewTree(nodes, db.NewMemDB())

	for i := 0; i <= nodes; i++ {
		k := randBytes(16)
		v := randBytes(32)
		tree.Set(k, v)
	}
	tree.Hash()
	return tree
}
