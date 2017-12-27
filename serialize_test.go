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
const TreeVariations = 1

func TestSerialize(t *testing.T) {
	require := require.New(t)

	cases := []struct {
		serializer Serializer
		sameHash   bool
	}{
		{&inOrderSerializer{}, true},
		{&breadthFirstSerializer{}, true},
	}

	for i, tc := range cases {
		for j := 0; j < TreeVariations; j++ {
			tree := makeRandomTree(TreeSize)
			serializer := tc.serializer
			stored := serializer.Serialize(tree, tree.root)
			require.NotNil(stored, "%d", i)
			require.Equal(tree.Size(), len(stored), "%d", i)
			origHash := tree.Hash()
			require.NotNil(origHash)

			empty := NewTree(nil, TreeSize)
			require.Equal(0, empty.Size(), "%d", i)
			serializer.Restore(empty, stored)
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
