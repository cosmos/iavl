package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tmlibs/db"
)

func TestSimpleChunk(t *testing.T) {
	require := require.New(t)

	// get the chunk info we use
	tree := makeRandomTree(TreeSize)
	hashes, _, _, err := getChunkHashes(tree, 0)
	require.NoError(err)
	require.Equal(1, len(hashes))
	hash := hashes[0]
	require.Equal(tree.Hash(), hash)

	// get tree as one chunk
	chunk := GetChunk(tree, 0, 0)
	require.NotEmpty(chunk)
	require.Equal(tree.Size(), len(chunk))

	// sort chunk and check integrity
	chunk.Sort()
	check := chunk.CalculateRoot()
	require.Equal(hash, check)

	// Trying to get chunks with a depth that exceeds the maximum allowed
	// returns an error.
	_, _, _, err = getChunkHashes(tree, 99)
	require.Error(err)
}

func TestMultipleChunks(t *testing.T) {
	require := require.New(t)

	cases := []struct {
		depth uint
	}{
		{0},
		{1},
		{4},
		{7},
	}

	// get the chunk info we use
	tree := makeRandomTree(TreeSize)

	for _, tc := range cases {
		hashes, _, depth, err := getChunkHashes(tree, tc.depth)
		require.NoError(err)
		numChunks := len(hashes)
		require.Equal(1<<depth, numChunks, "%d", tc.depth)

		var accum Chunk
		for i := 0; i < numChunks; i++ {
			// get tree as one chunk
			chunk := GetChunk(tree, depth, uint(i))
			require.NotEmpty(chunk, "%d/%d", depth, i)

			// sort chunk and check integrity
			chunk.Sort()
			check := chunk.CalculateRoot()
			require.Equal(hashes[i], check, "%d/%d", depth, i)

			// add this chunk to our accum
			accum = MergeChunks(accum, chunk)
		}

		require.Equal(tree.Size(), len(accum))
		final := accum.CalculateRoot()
		require.Equal(tree.Hash(), final, "%d", tc.depth)
	}
}

func TestChunkProofs(t *testing.T) {
	require := require.New(t)
	tree := makeAlphabetTree()

	hashes, proofs, _ := GetChunkHashesWithProofs(tree)
	require.NotEmpty(hashes)
	require.Equal(len(hashes), len(proofs))

	for i, hash := range hashes {
		err := proofs[i].Verify(hash, nil, tree.Hash())
		require.NoError(err)
	}
}

func makeAlphabetTree() *Tree {
	t := NewTree(db.NewMemDB(), 26)
	alpha := []byte("abcdefghijklmnopqrstuvwxyz")

	for _, a := range alpha {
		t.Set([]byte{a}, []byte{a})
	}
	t.Hash()

	return t
}
