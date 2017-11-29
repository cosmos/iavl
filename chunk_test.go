package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleChunk(t *testing.T) {
	require := require.New(t)

	// get the chunk info we use
	tree := makeRandomTree(TreeSize)
	hashes := GetChunkHashes(tree, 0)
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
