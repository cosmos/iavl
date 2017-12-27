package iavl

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tmlibs/db"
)

// TreeSize is the number of nodes in our test trees
const TreeSize = 10000

func TestSerialize(t *testing.T) {
	require := require.New(t)

	cases := []struct {
		name       string
		serializer Serializer
		tree       *Tree
	}{
		{"in-order-alpha", &inOrderSerializer{}, makeAlphabetTree()},
		{"bfs-alpha", &breadthFirstSerializer{}, makeAlphabetTree()},
		{"in-order", &inOrderSerializer{}, makeRandomTree(TreeSize)},
		{"bfs", &breadthFirstSerializer{}, makeRandomTree(TreeSize)},
		{"in-order-versioned", &inOrderSerializer{}, makeRandomVersionedTree(TreeSize)},
	}

	for i, tc := range cases {
		tree := tc.tree
		stored := tc.serializer.Serialize(tree, tree.root)
		require.NotNil(stored, "%d", i)
		require.Equal(tree.Size(), len(stored), "%d", i)
		origHash := tree.Hash()
		require.NotNil(origHash)

		empty := NewTree(nil, TreeSize)
		require.Equal(0, empty.Size(), "%d", i)
		tc.serializer.Restore(empty, stored)
		require.Equal(tree.Size(), empty.Size(), "%d", i)

		newHash := empty.Hash()
		require.NotNil(newHash)
		require.EqualValues(fmt.Sprintf("%x", origHash), fmt.Sprintf("%x", newHash), "hashes don't match for: %s\n%#v", tc.name, stored)
	}
}

// TODO: add some deletes in there as well?
func makeRandomTree(nodes int) *Tree {
	tree := NewTree(db.NewMemDB(), nodes)

	for i := 0; i <= nodes; i++ {
		k := []byte(randstr(8))
		v := k
		tree.Set(k, v)
	}
	tree.Hash()
	return tree
}

func makeRandomVersionedTree(nodes int) *Tree {
	tree := NewVersionedTree(db.NewMemDB(), nodes)

	for i := 0; i <= nodes; i++ {
		k := []byte(randstr(8))
		v := k
		tree.Set(k, v)

		if i%(nodes/100) == 0 {
			tree.SaveVersion()
		}
	}
	tree.Hash()
	return tree.Tree()
}

func BenchmarkSerialize(b *testing.B) {
	cases := []struct {
		name       string
		serializer Serializer
		sameHash   bool
	}{
		{"in-order", &inOrderSerializer{}, true},
		{"bfs", &breadthFirstSerializer{}, true},
	}

	treeSizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range treeSizes {
		tree := makeRandomTree(size)
		origHash := tree.Hash()
		for _, tc := range cases {
			b.Run(fmt.Sprintf("%s-%d", tc.name, size), func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					serializer := tc.serializer
					stored := serializer.Serialize(tree, tree.root)
					empty := NewTree(nil, size)
					serializer.Restore(empty, stored)

					if !bytes.Equal(empty.Hash(), origHash) {
						panic("Tree hashes don't match!")
					}
				}
			})
		}
	}
}
