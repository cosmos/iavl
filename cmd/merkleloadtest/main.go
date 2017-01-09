package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"

	db "github.com/tendermint/go-db"
	merkle "github.com/tendermint/go-merkle"
)

const reportInterval = 1000

// blatently copied from benchmarks/bench_test.go
func randBytes(length int) []byte {
	key := make([]byte, length)
	// math.rand.Read always returns err=nil
	rand.Read(key)
	return key
}

// blatently copied from benchmarks/bench_test.go
func prepareTree(db db.DB, size, keyLen, dataLen int) (merkle.Tree, [][]byte) {
	t := merkle.NewIAVLTree(size, db)
	keys := make([][]byte, size)

	for i := 0; i < size; i++ {
		key := randBytes(keyLen)
		t.Set(key, randBytes(dataLen))
		keys[i] = key
	}
	t.Hash()
	t.Save()
	runtime.GC()
	return t, keys
}

func runBlock(t merkle.Tree, dataLen, blockSize int, keys [][]byte) merkle.Tree {
	l := int32(len(keys))

	real := t.Copy()
	check := t.Copy()

	for j := 0; j < blockSize; j++ {
		// always update to avoid changing size
		key := keys[rand.Int31n(l)]
		data := randBytes(dataLen)

		// perform query and write on check and then real
		check.Get(key)
		check.Set(key, data)
		real.Get(key)
		real.Set(key, data)
	}

	// at the end of a block, move it all along....
	real.Hash()
	real.Save()
	return real
}

func loopForever(t merkle.Tree, dataLen, blockSize int, keys [][]byte) {
	for {
		start := time.Now()
		for i := 0; i < reportInterval; i++ {
			t = runBlock(t, dataLen, blockSize, keys)
		}
		// now report
		end := time.Now()
		delta := end.Sub(start)
		timing := delta.Seconds() / reportInterval
		fmt.Printf("%s: blocks of %d tx: %0.3f s/block\n",
			end.Format("Jan 2 15:04:05"), blockSize, timing)
	}
}

func main() {
	// TODO: configure
	initSize := 100 * 1000
	keySize := 16
	dataSize := 100
	blockSize := 200
	dbtype := "memdb"

	tmpDir := "/tmp/foo"

	fmt.Printf("Preparing DB (%s with %d keys)...\n", dbtype, initSize)
	d := db.NewDB("loadtest", dbtype, tmpDir)
	tree, keys := prepareTree(d, initSize, keySize, dataSize)
	fmt.Printf("Keysize: %d, Datasize: %d\n", keySize, dataSize)

	fmt.Printf("Starting loadtest (blocks of %d tx)...\n", blockSize)
	loopForever(tree, dataSize, blockSize, keys)
}
