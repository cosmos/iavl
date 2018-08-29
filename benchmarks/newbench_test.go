package benchmarks

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/tendermint/iavl"
	db "github.com/tendermint/tendermint/libs/db"
)

var dbChoices = []db.DBBackendType{"memdb", "goleveldb"}

func MakeTree(keySize int, dataSize int, numItems int, dataBase db.DB, cacheSize int) [][]byte {
	rand.Seed(123456789)
	t := iavl.NewMutableTree(dataBase, cacheSize)
	keys := make([][]byte, numItems)
	for i := 0; i < numItems; i++ {
		k := randBytes(keySize)
		keys[i] = k
		t.Set(k, randBytes(dataSize))
	}
	t.SaveVersion()
	return keys
}

func getKeyNotInTree(dataBase db.DB, keySize int) []byte {
	tree := iavl.NewMutableTree(dataBase, 0)
	tree.Load()
	for {
		key := randBytes(keySize)
		if tree.Has(key) == false {
			return key
		}
	}
}

func getKeyInTree(keys [][]byte) []byte {
	return keys[rand.Intn(len(keys))]
}

// Benchmarks setting a specific key/value pair in a mutable tree loaded from a db
func benchmarkSet(b *testing.B, key []byte, value []byte, dataBase db.DB, cacheSize int) {
	tree := iavl.NewMutableTree(dataBase, cacheSize)
	tree.Load()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//b.StartTimer()
		tree.Set(key, value)
		//b.StopTimer()
		tree.Rollback()
	}
}

func benchmarkRemove(b *testing.B, key []byte, dataBase db.DB, cacheSize int) {
	tree := iavl.NewMutableTree(dataBase, cacheSize)
	tree.Load()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Remove(key)
		tree.Rollback()
	}
}

func mkRange(start uint, end uint, step uint) []uint {
	size := uint(math.Ceil(float64(end-start) / float64(step)))
	ret := make([]uint, size)
	for i := uint(0); i < size; i++ {
		ret[i] = start + (i * step)
	}
	return ret
}

func mkRangeHelper(ranges [][]uint, cur [][]uint) [][]uint {
	if len(ranges) == 0 {
		return cur
	}
	todo := ranges[0]
	parts := mkRange(todo[0], todo[1], todo[2])
	newsize := len(parts) * len(cur)
	nxt := make([][]uint, newsize)
	for i := 0; i < len(parts); i++ {
		for j := 0; j < len(cur); j++ {
			nc := make([]uint, len(cur[j])+1)
			copy(nc, cur[j])
			nc[len(cur[j])] = parts[i]
			nxt[j+(i*len(cur))] = nc
		}
	}
	return mkRangeHelper(ranges[1:], nxt)
}

func MakeMultiRange(ranges [][]uint) [][]uint {
	return mkRangeHelper(ranges, [][]uint{{}})
}

//for debugging the multi range constructor
// func BenchmarkTryMultiRange(b *testing.B) {
// 	fmt.Println(MakeMultiRange([][]uint{{5, 16, 5}, {5, 16, 5}, {10, 11, 5}, {10, 11, 5}, {10, 11, 5}, {10, 11, 5}, {0, 2, 1}}))
// }

func BenchmarkInsert(b *testing.B) {
	logTreeSizeRange := []uint{5, 21, 5}
	logCacheSizeRange := []uint{0, 21, 5}
	logInsertedKeySize := []uint{5, 11, 5}
	logInsertedDataSize := []uint{5, 11, 5}
	logCurrentKeySize := []uint{5, 11, 5}
	logCurrentDataSize := []uint{5, 11, 5}
	choiceDataBase := []uint{0, 2, 1}
	fmt.Println("tree, cache, update data, current key, current data, db")
	params := MakeMultiRange([][]uint{
		logTreeSizeRange, logCacheSizeRange, logInsertedKeySize,
		logInsertedDataSize, logCurrentKeySize, logCurrentDataSize,
		choiceDataBase,
	})
	fmt.Printf("number of datapoints to collect: %d\n", len(params))
	for x, p := range params {
		lts := p[0]
		lcs := p[1]
		liks := p[2]
		lids := p[3]
		lcks := p[4]
		lcds := p[5]
		cdb := dbChoices[p[6]]

		dirName := "BenchData/test.db"

		dataBase := db.NewDB("test", cdb, dirName)
		MakeTree(1<<lcks, 1<<lcds, 1<<lts, dataBase, 1<<lcs)
		key := getKeyNotInTree(dataBase, 1<<liks)
		data := randBytes(1 << lids)
		b.Run(fmt.Sprintf("Insert - %d (%d %d %d %d %d %d %s)", x, lts, lcs, liks, lids, lcks, lcds, cdb), func(b *testing.B) {
			benchmarkSet(b, key, data, dataBase, 1<<lcs)
		})
		dataBase.Close()
		os.RemoveAll(dirName)
	}
}

func BenchmarkUpdate(b *testing.B) {
	logTreeSizeRange := []uint{5, 21, 5}
	logCacheSizeRange := []uint{0, 21, 5}
	logUpdatedDataSize := []uint{5, 11, 5}
	logCurrentKeySize := []uint{5, 11, 5}
	logCurrentDataSize := []uint{5, 11, 5}
	choiceDataBase := []uint{0, 2, 1}
	fmt.Println("tree, cache, current key, current data, db")
	params := MakeMultiRange([][]uint{
		logTreeSizeRange, logCacheSizeRange,
		logUpdatedDataSize, logCurrentKeySize, logCurrentDataSize,
		choiceDataBase,
	})
	fmt.Printf("number of datapoints to collect: %d\n", len(params))
	for x, p := range params {
		lts := p[0]
		lcs := p[1]
		luds := p[2]
		lcks := p[3]
		lcds := p[4]
		cdb := dbChoices[p[5]]

		dirName := "BenchData/test.db"

		dataBase := db.NewDB("test", cdb, dirName)
		keys := MakeTree(1<<lcks, 1<<lcds, 1<<lts, dataBase, 1<<lcs)
		key := getKeyInTree(keys)
		data := randBytes(1 << luds)
		b.Run(fmt.Sprintf("Update - %d (%d %d %d %d %d %s)", x, lts, lcs, luds, lcks, lcds, cdb), func(b *testing.B) {
			benchmarkSet(b, key, data, dataBase, 1<<lcs)
		})
		dataBase.Close()
		os.RemoveAll(dirName)
	}
}

func BenchmarkRemove(b *testing.B) {
	logTreeSizeRange := []uint{5, 21, 5}
	logCacheSizeRange := []uint{0, 21, 5}
	logCurrentKeySize := []uint{5, 11, 5}
	logCurrentDataSize := []uint{5, 11, 5}
	choiceDataBase := []uint{0, 2, 1}
	fmt.Println("tree, cache, current key, current data, db")
	params := MakeMultiRange([][]uint{
		logTreeSizeRange, logCacheSizeRange, logCurrentKeySize, logCurrentDataSize,
		choiceDataBase,
	})
	fmt.Printf("number of datapoints to collect: %d\n", len(params))
	for x, p := range params {
		lts := p[0]
		lcs := p[1]
		lcks := p[2]
		lcds := p[3]
		cdb := dbChoices[p[4]]

		dirName := "BenchData/test.db"

		dataBase := db.NewDB("test", cdb, dirName)
		keys := MakeTree(1<<lcks, 1<<lcds, 1<<lts, dataBase, 1<<lcs)
		key := getKeyInTree(keys)
		b.Run(fmt.Sprintf("Remove - %d (%d %d %d %d %s)", x, lts, lcs, lcks, lcds, cdb), func(b *testing.B) {
			benchmarkRemove(b, key, dataBase, 1<<lcs)
		})
		dataBase.Close()
		os.RemoveAll(dirName)
	}
}

/*

Actions to benchmark:
	Insert(tree, cache, keyi, datai, keyc, datac)
	Update(tree, cache, keyc, datac)
	Remove(tree, cache, keyc, datac)
	QueryHit(tree, cache, keyc, datac)
	QueryMiss(tree, cache, keyc, datac, keyq)
	Iterate Range(tree, cache, keyc, datac, range)

	Update many and SaveVersion
	DeleteVersion


Numeric Parameters:
	Num items in tree
	Key length of item in question
	Data length of item in question
	Key length distribution in tree
	Data length distribution in tree

Choices
	DB backend



*/
