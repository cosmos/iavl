package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleChunk(t *testing.T) {
	require := require.New(t)

	// get the chunk info we use
	tree := makeRandomTree(TreeSize)
	hashes, _ := GetChunkHashes(tree, 0)
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
		{16},
	}

	// get the chunk info we use
	tree := makeRandomTree(TreeSize)

	for _, tc := range cases {
		hashes, depth := GetChunkHashes(tree, tc.depth)
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
