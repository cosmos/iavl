package iavl

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/db"
	"log"
	"math"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	numBlocks    = 10
	blockSize    = 10
	maxLogDbSize = 8
)

func TestFuzzTestSaveVersionToDB(t *testing.T) {
	t.Skip()
	tree := NewMutableTree(db.NewMemDB(), 0)
	historicBlocks := generateBlocks(numBlocks, blockSize)
	runBlocks(t, tree, historicBlocks)

	newDb := db.NewMemDB()
	_, _, err := tree.SaveVersionToDB(0, newDb)
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

func TestBenchmarkSaveVersionToDB(t *testing.T) {
	t.Skip()
	for logDbSize := 1; logDbSize < maxLogDbSize; logDbSize++ {
		dbSize := int(math.Pow(10, float64(logDbSize)))
		transactions := genSetProgram(dbSize)

		originalDbName := "dbs/testOrignalDb_" + strconv.Itoa(dbSize)
		_ = os.Remove(originalDbName)
		originalDb, err := db.NewGoLevelDB(originalDbName, ".")
		require.NoError(t, err)
		tree := NewMutableTree(originalDb, 0)

		_, _, err = tree.SaveVersion()
		_, _, err = tree.SaveVersion()
		require.NoError(t, transactions.Execute(tree))
		_, _, err = tree.SaveVersion()

		newDbName := "dbs/newDb_" + strconv.Itoa(dbSize)
		_ = os.Remove(newDbName)
		newDb, err := db.NewGoLevelDB(newDbName, ".")
		require.NoError(t, err)

		start := time.Now()
		_, _, err = tree.SaveVersionToDB(0, newDb)
		now := time.Now()
		elapsed := now.Sub(start)

		require.NoError(t, err)
		fmt.Printf(
			"benchmarke SaveVersionToDB" + fmt.Sprintf("\tnum transactions %v\teleapsed time %v\n",
				dbSize,
				elapsed,
			))

		newTree := NewMutableTree(newDb, 0)
		_, err = newTree.LoadVersion(tree.Version())
		futureBlocks := generateBlocks(numBlocks, blockSize)
		runBlocks(t, tree, futureBlocks)
		runBlocks(t, newTree, futureBlocks)
		require.Equal(t, 0, bytes.Compare(tree.Hash(), newTree.Hash()))
		require.Equal(t, tree.Version(), newTree.Version())

	}
}

// Generate a random program of the given size.
func genSetProgram(size int) *program {
	p := &program{}

	for p.size() < size {
		k, v := []byte(common.RandStr(1)), []byte(common.RandStr(1))
		p.addInstruction(instruction{op: "SET", k: k, v: v})
	}
	return p
}

func TestCompaireTimeVersionToDB(t *testing.T) {
	t.Skip()
	var err error
	tree := NewMutableTree(db.NewMemDB(), 0)
	historicBlocks := generateBlocks(numBlocks, blockSize)
	runBlocks(t, tree, historicBlocks)

	start := time.Now()

	newDb := db.NewMemDB()
	_, _, err = tree.SaveVersionToDB(tree.Version(), newDb)
	require.NoError(t, err)
	newTree := NewMutableTree(newDb, 0)
	_, err = newTree.LoadVersion(tree.Version())
	require.NoError(t, err)
	now := time.Now()
	elapsed := now.Sub(start)
	fmt.Println("Time to slice tree ", elapsed, " blocks ", numBlocks, " of size ", blockSize)

	start = time.Now()
	for version := 1; version < int(tree.Version()); version++ {
		if err := tree.DeleteVersion(int64(version)); err != nil {
			fmt.Println("version ", version, " err", err)
		}
	}
	now = time.Now()
	elapsed = now.Sub(start)
	fmt.Println("Time to delete tree version by version ", elapsed, " blocks ", numBlocks, " of size ", blockSize)

	require.NoError(t, err)

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

func TestInterate(t *testing.T) {
	t.Skip()
	memDb := db.NewMemDB()
	mutTree := NewMutableTree(memDb, .0)

	_ = mutTree.Set([]byte("#alice"), []byte("abc"))
	_ = mutTree.Set([]byte("#jane"), []byte("yellow"))
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
	_ = mutTree.Set([]byte("#alice"), []byte("cccc"))
	_ = mutTree.Set([]byte("#fred"), []byte("zzzz"))
	_ = mutTree.Set([]byte("#mary"), []byte("zzzz"))
	_, _, err = mutTree.SaveVersion()
	require.NoError(t, err)

	numKeys := uint64(0)
	keyTotal := uint64(0)
	valueTotal := uint64(0)
	//memDb.Print()

	mutTree.IterateRangeInclusive(
		nil,
		nil,
		true,
		func(key, value []byte, version int64) bool {
			numKeys++
			keyTotal += uint64(len(key))
			valueTotal += uint64(len(value))
			//if logPeriod > 0 && numKeys % logPeriod == logPeriod-1 {
			log.Printf("numKeys %v\tkeys tmem%v\tvalue mem%v", numKeys, keyTotal, valueTotal)
			log.Printf("key %v\tvalue%v\tversion%v", string(key), string(value), version)
			//}
			return false
		},
	)
}

func TestSaveVersionToDB(t *testing.T) {
	//t.Skip()
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
	/*
		newMemDb1 := db.NewMemDB()
		fmt.Println("old save");
		newHash1, newVersion1, err := mutTree.SaveVersionToDBOld(mutTree.Version(), newMemDb1); newHash1 = newHash1; newVersion1 = newVersion1
		fmt.Println("--------------------------------------------------");
		require.NoError(t, err)
		newTree1 := NewMutableTree(newMemDb1, 0)
		_, err = newTree1.LoadVersion(mutTree.Version())
		require.NoError(t, err)
		require.Equal(t, mutTree.Version(), newVersion1)
		require.Equal(t, 0, bytes.Compare(mutTree.Hash(), newTree1.Hash()))
	*/
	//mutTree.Iterate(func(key []byte, value []byte) bool{
	//	fmt.Printf("iterate key %v value %v\n", string(key), string(value))
	//	return false
	//})

	newMemDb := db.NewMemDB()
	fmt.Println()
	fmt.Println("new save")
	_, newVersion, err := mutTree.SaveVersionToDB(mutTree.Version(), newMemDb)
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
	_, _, err = mutTree.SaveVersionToDB(oldVersion, newNewMemDB)
	require.NoError(t, err)
	newOldTree := NewMutableTree(newNewMemDB, 0)
	_, err = newOldTree.LoadVersion(oldVersion)
	require.NoError(t, err)
	require.Equal(t, oldVersion, newOldTree.Version())
	oldHash = oldHash
	//require.Equal(t, 0, bytes.Compare(oldHash, newOldTree.Hash()))
}
