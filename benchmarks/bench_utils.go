package benchmarks

import (
	"math"
	"math/rand"

	"github.com/tendermint/iavl"
	db "github.com/tendermint/tendermint/libs/db"
)

var dirName = "BenchData/test.db"

func MakeTree(keySize int, dataSize int, numItems int, cacheSize int, dbType db.DBBackendType) ([][]byte, db.DB) {
	rand.Seed(123456789)
	dataBase := db.NewDB("test", dbType, dirName)
	t := iavl.NewMutableTree(dataBase, cacheSize)
	keys := make([][]byte, numItems)
	for i := 0; i < numItems; i++ {
		k := randBytes(keySize)
		keys[i] = k
		t.Set(k, randBytes(dataSize))
	}
	t.SaveVersion()
	return keys, dataBase
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
