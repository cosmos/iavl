package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TreeSize is the number of nodes in our test trees
const TreeSize = 1000

func TestSerialize(t *testing.T) {
	require := require.New(t)

	cases := []struct {
		algo     SerializeFunc
		sameHash bool
	}{
		{InOrderSerializer, false},
	}

	for i, tc := range cases {
		tree := makeRandomTree(TreeSize)
		stored := tc.algo(tree)
		require.NotNil(stored, "%d", i)
		require.Equal(len(stored), tree.Size(), "%d", i)
		origHash := tree.Hash()

		empty := NewTree(TreeSize, nil)
		require.Equal(0, empty.Size(), "%d", i)
		Restore(empty, stored)
		require.Equal(len(stored), empty.Size(), "%d", i)

		newHash := empty.Hash()
		if tc.sameHash {
			require.Equal(origHash, newHash, "%d", i)
		} else {
			require.NotEqual(origHash, newHash, "%d", i)
		}
	}
}

// TODO: add some deletes in there as well?
func makeRandomTree(nodes int) *Tree {
	tree := NewTree(nodes, nil)
	for i := 0; i <= nodes; i++ {
		k := randBytes(16)
		v := randBytes(32)
		tree.Set(k, v)
	}
	return tree
}
