package benchmarks

import (
	"fmt"
	"os"
	"testing"

	"github.com/tendermint/iavl"
	db "github.com/tendermint/tendermint/libs/db"
)

// Benchmarks setting a specific key/value pair in a mutable tree loaded from a db
func benchmarkSet(b *testing.B, key []byte, value []byte, dataBase db.DB, cacheSize int) {
	tree := iavl.NewMutableTree(dataBase, cacheSize)
	tree.Load()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Set(key, value)
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

func benchmarkGet(b *testing.B, key []byte, dataBase db.DB, cacheSize int) {
	tree := iavl.NewMutableTree(dataBase, cacheSize)
	tree.Load()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(key)
	}
}

var logTreeSizeRange = []uint{5, 21, 7}
var logTreeKeySize = []uint{5, 11, 5}
var logTreeDataSize = []uint{5, 11, 5}
var logCacheSizeRange = []uint{0, 15, 7}
var logArgKeySize = []uint{5, 11, 5}
var logArgDataSize = []uint{5, 11, 5}
var choiceDataBase = []uint{0, 2, 1}
var dbChoices = []db.DBBackendType{"memdb", "goleveldb"}

func BenchmarkInsert(b *testing.B) {
	fmt.Println("tree, cache, update data, current key, current data, db")
	params := MakeMultiRange([][]uint{
		logTreeSizeRange, logCacheSizeRange, logArgKeySize,
		logArgDataSize, logTreeKeySize, logTreeDataSize,
		choiceDataBase,
	})
	fmt.Printf("number of datapoints to collect: %d\n", len(params))
	for x, p := range params {
		lts, lcs, laks, lads, ltks, ltds, cdb := p[0], p[1], p[2], p[3], p[4], p[5], dbChoices[p[6]]
		_, dataBase := MakeTree(1<<ltks, 1<<ltds, 1<<lts, 1<<lcs, cdb)
		key := getKeyNotInTree(dataBase, 1<<laks)
		data := randBytes(1 << lads)
		b.Run(fmt.Sprintf("%d (%d %d %d %d %d %d %s)", x, lts, lcs, laks, lads, ltks, ltds, cdb), func(b *testing.B) {
			benchmarkSet(b, key, data, dataBase, 1<<lcs)
		})
		dataBase.Close()
		os.RemoveAll(dirName)
	}
}

func BenchmarkUpdate(b *testing.B) {
	fmt.Println("tree, cache, current key, current data, db")
	params := MakeMultiRange([][]uint{
		logTreeSizeRange, logCacheSizeRange,
		logArgDataSize, logTreeKeySize, logTreeDataSize,
		choiceDataBase,
	})
	fmt.Printf("number of datapoints to collect: %d\n", len(params))
	for x, p := range params {
		lts, lcs, lads, ltks, ltds, cdb := p[0], p[1], p[2], p[3], p[4], dbChoices[p[5]]
		keys, dataBase := MakeTree(1<<ltks, 1<<ltds, 1<<lts, 1<<lcs, cdb)
		key := getKeyInTree(keys)
		data := randBytes(1 << lads)
		b.Run(fmt.Sprintf("%d (%d %d %d %d %d %s)", x, lts, lcs, lads, ltks, ltds, cdb), func(b *testing.B) {
			benchmarkSet(b, key, data, dataBase, 1<<lcs)
		})
		dataBase.Close()
		os.RemoveAll(dirName)
	}
}

func BenchmarkRemove(b *testing.B) {
	fmt.Println("tree, cache, tree key, tree data, db")
	params := MakeMultiRange([][]uint{
		logTreeSizeRange, logCacheSizeRange, logTreeKeySize, logTreeDataSize,
		choiceDataBase,
	})
	fmt.Printf("number of datapoints to collect: %d\n", len(params))
	for x, p := range params {
		lts, lcs, ltks, ltds, cdb := p[0], p[1], p[2], p[3], dbChoices[p[4]]
		keys, dataBase := MakeTree(1<<ltks, 1<<ltds, 1<<lts, 1<<lcs, cdb)
		key := getKeyInTree(keys)
		b.Run(fmt.Sprintf("%d (%d %d %d %d %s)", x, lts, lcs, ltks, ltds, cdb), func(b *testing.B) {
			benchmarkRemove(b, key, dataBase, 1<<lcs)
		})
		dataBase.Close()
		os.RemoveAll(dirName)
	}
}

func BenchmarkQueryHit(b *testing.B) {
	fmt.Println("tree, cache, tree key, tree data, db")
	params := MakeMultiRange([][]uint{
		logTreeSizeRange, logCacheSizeRange, logTreeKeySize, logTreeDataSize,
		choiceDataBase,
	})
	fmt.Printf("number of datapoints to collect: %d\n", len(params))
	for x, p := range params {
		lts, lcs, ltks, ltds, cdb := p[0], p[1], p[2], p[3], dbChoices[p[4]]
		keys, dataBase := MakeTree(1<<ltks, 1<<ltds, 1<<lts, 1<<lcs, cdb)
		key := getKeyInTree(keys)
		b.Run(fmt.Sprintf("%d (%d %d %d %d %s)", x, lts, lcs, ltks, ltds, cdb), func(b *testing.B) {
			benchmarkGet(b, key, dataBase, 1<<lcs)
		})
		dataBase.Close()
		os.RemoveAll(dirName)
	}
}

func BenchmarkQueryMiss(b *testing.B) {
	fmt.Println("tree, cache, tree key, tree data, db")
	params := MakeMultiRange([][]uint{
		logTreeSizeRange, logCacheSizeRange, logTreeKeySize, logTreeDataSize,
		choiceDataBase,
	})
	fmt.Printf("number of datapoints to collect: %d\n", len(params))
	for x, p := range params {
		lts, lcs, ltks, ltds, cdb := p[0], p[1], p[2], p[3], dbChoices[p[4]]
		_, dataBase := MakeTree(1<<ltks, 1<<ltds, 1<<lts, 1<<lcs, cdb)
		key := getKeyNotInTree(dataBase, 1<<ltks)
		b.Run(fmt.Sprintf("%d (%d %d %d %d %s)", x, lts, lcs, ltks, ltds, cdb), func(b *testing.B) {
			benchmarkGet(b, key, dataBase, 1<<lcs)
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
