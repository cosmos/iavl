package iavl

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/libs/db"
	"testing"
)

const (
	numBlocks = 10
	blockSize = 10
)

func TestFuzzNewSlice(t *testing.T) {
	tree := NewMutableTree(db.NewMemDB(), 0)
	historicBlocks := generateBlocks(numBlocks, blockSize)
	runBlocks(t, tree, historicBlocks)

	_, _, newTree, err := tree.NewSliceAt(tree.Version(), db.NewMemDB())
	require.NoError(t, err)

	futureBlocks := generateBlocks(numBlocks, blockSize)
	runBlocks(t, tree, futureBlocks)
	runBlocks(t, newTree, futureBlocks)
	require.Equal(t, 0, bytes.Compare(tree.Hash(), newTree.Hash()))
	require.Equal(t, tree.Version(), newTree.Version())
}

func generateBlocks(numBlocks, blockSize int) []*program {
	var history []*program
	for i := 0; i < numBlocks; i++ {
		history = append(history, genRandomProgram(blockSize))
	}
	return history
}

func runBlocks(t *testing.T, tree *MutableTree, blocks []*program) {
	for _, block := range blocks {
		if err := block.Execute(tree); err != nil {
			require.NoError(t, err)
		}
		if _, _, err := tree.SaveVersion(); err != nil {
			require.NoError(t, err)
		}
	}
}

func TestNewSlice(t *testing.T) {
	memDb := db.NewMemDB()
	mutTree := NewMutableTree(memDb, .0)

	_ = mutTree.Set([]byte("#alice"), []byte("abc"))
	hash, version, err := mutTree.SaveVersion()
	require.NoError(t, err)

	_ = mutTree.Set([]byte("#bob"), []byte("pqr"))
	_ = mutTree.Set([]byte("#alice"), []byte("ass"))
	hash, version, err = mutTree.SaveVersion()
	hash = hash
	version = version
	require.NoError(t, err)

	_ = mutTree.Set([]byte("#alice"), []byte("xyz"))
	_, _ = mutTree.Remove([]byte("#bob"))
	_ = mutTree.Set([]byte("#alice"), []byte("zzzz"))
	_ = mutTree.Set([]byte("#fred"), []byte("zzzz"))
	_ = mutTree.Set([]byte("#mary"), []byte("zzzz"))
	oldHash, oldVersion, err := mutTree.SaveVersion()
	require.NoError(t, err)

	newMemDb := db.NewMemDB()
	_, newVersion, newTree, err := mutTree.NewSliceAt(mutTree.Version(), newMemDb)

	require.Equal(t, mutTree.Version(), newVersion)
	require.Equal(t, 0, bytes.Compare(mutTree.Hash(), newTree.Hash()))

	mutTree.Set([]byte("#sally"), []byte("xxx"))
	newTree.Set([]byte("#sally"), []byte("xxx"))

	oldTreeHash, _, err := mutTree.SaveVersion()
	require.NoError(t, err)
	newTreeHash, _, err := newTree.SaveVersion()
	require.NoError(t, err)

	require.Equal(t, 0, bytes.Compare(oldTreeHash, newTreeHash))

	keys, values, _, err := mutTree.GetRangeWithProof([]byte("#"), nil, 0)
	require.NoError(t, err)
	require.Equal(t, len(keys), len(values))
	newKeys, newValues, _, err := newTree.GetRangeWithProof([]byte("#"), nil, 0)
	require.NoError(t, err)
	require.Equal(t, len(newKeys), len(newValues))
	require.Equal(t, len(keys), len(newKeys))
	for i := range keys {
		require.Equal(t, 0, bytes.Compare(keys[i], newKeys[i]))
		require.Equal(t, 0, bytes.Compare(values[i], newValues[i]))
	}

	newNewMemDB := db.NewMemDB()
	_, _, newOldTree, err := mutTree.NewSliceAt(oldVersion, newNewMemDB)
	require.Equal(t, oldVersion, newOldTree.Version())
	require.Equal(t, 0, bytes.Compare(oldHash, newOldTree.Hash()))

}
