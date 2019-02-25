package iavl

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/libs/db"
	"testing"
)

const (
	numBlocks = 10
	blockSize = 10
)

func TestFuzzTestSaveVersionToDB(t *testing.T) {
	tree := NewMutableTree(db.NewMemDB(), 0)
	historicBlocks := generateBlocks(numBlocks, blockSize)
	runBlocks(t, tree, historicBlocks)

	newDb := db.NewMemDB()
	_, _, err := tree.SaveVersionToDB(0, newDb, 5, nil)
	require.NoError(t, err)
	newTree := NewMutableTree(newDb, 0)
	_, err = newTree.LoadVersion(tree.Version())
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
		require.NoError(t, block.Execute(tree))
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
}

func TestSaveVersionToDB(t *testing.T) {
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
	_, newVersion, err := mutTree.SaveVersionToDB(mutTree.Version(), newMemDb, 5, nil)
	fmt.Println("--------------------------------------------------")
	require.NoError(t, err)
	newTree := NewMutableTree(newMemDb, 0)
	_, err = newTree.LoadVersion(mutTree.Version())
	require.NoError(t, err)

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
	_, _, err = mutTree.SaveVersionToDB(oldVersion, newNewMemDB, 5, nil)
	require.NoError(t, err)
	newOldTree := NewMutableTree(newNewMemDB, 0)
	_, err = newOldTree.LoadVersion(oldVersion)
	require.NoError(t, err)
	require.Equal(t, oldVersion, newOldTree.Version())

	oldHash = oldHash
	oldVersion = oldVersion

}
