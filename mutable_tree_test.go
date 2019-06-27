package iavl

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/db"
)

const (
	numBlocks = 10
	blockSize = 10
)

func TestDelete(t *testing.T) {
	memDb := db.NewMemDB()
	tree := NewMutableTree(memDb, 0)

	tree.set([]byte("k1"), []byte("Fred"))
	hash, version, err := tree.SaveVersion()
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	require.NoError(t, tree.DeleteVersion(version))

	k1Value, _, err := tree.GetVersionedWithProof([]byte("k1"), version)
	require.Nil(t, k1Value)

	//key := tree.ndb.rootKey(version)
	key := rootKeyFormat.Key(version)
	memDb.Set(key, hash)
	tree.versions[version] = true

	k1Value, _, err = tree.GetVersionedWithProof([]byte("k1"), version)
	require.Equal(t, 0, bytes.Compare([]byte("Fred"), k1Value))
	PrintTree(tree.ImmutableTree)
}

func TestFuzzTestSaveVersionToDB(t *testing.T) {
	t.Skip()
	old := db.NewMemDB()
	tree := NewMutableTree(old, 0)
	historicBlocks := generateBlocks(numBlocks, blockSize)
	runBlocks(t, tree, historicBlocks)

	newDb := db.NewMemDB()
	newNdb := NewNodeDB(newDb, 10000, nil)

	old.Print()
	fmt.Println()
	_, cloneVersion, err := tree.SaveVersionToDB(0, newNdb, 5, nil)
	fmt.Println("after old----------------------------------------------")
	old.Print()

	require.NoError(t, err)
	fmt.Println("after new----------------------------------------------")
	newDb.Print()
	cloneTree := NewMutableTree(newDb, 0)
	_, err = cloneTree.LoadVersion(cloneVersion)
	require.NoError(t, err)

	futureBlocks := generateBlocks(numBlocks, blockSize)
	runBlocks(t, tree, futureBlocks)
	runBlocks(t, cloneTree, futureBlocks)

	for version := cloneVersion; version < cloneVersion+numBlocks; version++ {
		_, err := tree.LoadVersion(version)
		require.NoError(t, err)
		_, err = cloneTree.LoadVersion(version)
		require.NoError(t, err)
		require.Equal(t, 0, bytes.Compare(tree.Hash(), cloneTree.Hash()))
	}
}

func TestFuzzTestVersions(t *testing.T) {
	t.Skip()
	memDb := db.NewMemDB()
	tree := NewMutableTree(memDb, 0)

	blockTxs := generateBlocks(numBlocks, blockSize)
	runBlocks(t, tree, blockTxs)

	for version := int64(2); version < numBlocks; version++ {
		cloneDb := db.NewMemDB()
		cloneNdb := NewNodeDB(cloneDb, 10000, nil)
		_, _, err := tree.SaveVersionToDB(version, cloneNdb, 5, nil)
		require.NoError(t, err)
		cloneTree := NewMutableTree(cloneDb, 0)
		_, err = cloneTree.LoadVersion(version)
		require.NoError(t, err)
		for block := version; block < numBlocks; block++ {
			require.NoError(t, blockTxs[block].Execute(cloneTree))
			_, saveVersion, err := cloneTree.SaveVersion()
			require.NoError(t, err)

			_, err = cloneTree.LoadVersion(saveVersion)
			require.NoError(t, err)
			_, err = tree.LoadVersion(saveVersion)
			require.NoError(t, err)

			require.Equal(t, 0, bytes.Compare(tree.Hash(), cloneTree.Hash()))
		}
	}
}

func generateBlocks(numBlocks, blockSize int) []*Program {
	var history []*Program
	for i := 0; i < numBlocks; i++ {
		history = append(history, genRandomProgramNoSave(blockSize))
	}
	return history
}

func runBlocks(t *testing.T, tree *MutableTree, blocks []*Program) {
	for _, block := range blocks {
		require.NoError(t, block.Execute(tree))
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
}

func TestSaveVersionToDB(t *testing.T) {
	t.Skip()
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

	mutTree.PrintDiskDb()
	fmt.Println("mut tree printed -------------")

	newMemDb := db.NewMemDB()
	newMemNdb := NewNodeDB(newMemDb, 10000, nil)
	_, newVersion, err := mutTree.SaveVersionToDB(mutTree.Version(), newMemNdb, 5, nil)
	fmt.Println("--------------------------------------------------")
	require.NoError(t, err)
	newTree := NewMutableTree(newMemDb, 0)

	fmt.Println("newTree printed -------------")
	newTree.PrintDiskDb()

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
	newNewNdb := NewNodeDB(newNewMemDB, 10000, nil)
	_, _, err = mutTree.SaveVersionToDB(oldVersion, newNewNdb, 5, nil)
	require.NoError(t, err)
	newOldTree := NewMutableTree(newNewMemDB, 0)
	_, err = newOldTree.LoadVersion(oldVersion)
	require.NoError(t, err)
	require.Equal(t, oldVersion, newOldTree.Version())

	oldHash = oldHash
	oldVersion = oldVersion

}

// Generate a random program of the given size.
func genRandomProgramNoSave(size int) *Program {
	p := &Program{}
	nextVersion := 1

	for p.size() < size {
		k, v := []byte(common.RandStr(1)), []byte(common.RandStr(1))

		switch common.RandInt() % 7 {
		case 0, 1, 2:
			p.addInstruction(instruction{op: "SET", k: k, v: v})
		case 3, 4, 5:
			p.addInstruction(instruction{op: "REMOVE", k: k})
		case 6:
			if rv := common.RandInt() % nextVersion; rv < nextVersion && rv > 0 {
				p.addInstruction(instruction{op: "DELETE", version: int64(rv)})
			}
		}
	}
	return p
}
