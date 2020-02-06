package benchmarks

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"

	db "github.com/tendermint/tm-db"
)

type pruningstrat struct {
	keepEvery, keepRecent int64
}

// To test effect of pruning strategy, we must measure time to execute many blocks
// Execute 30000 blocks with the given IAVL tree's pruning strategy
func runBlockChain(b *testing.B, prefix string, keepEvery int64, keepRecent int64, keyLen, dataLen int) {
	// prepare a dir for the db and cleanup afterwards
	dirName := fmt.Sprintf("./%s-db", prefix)
	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			b.Errorf("%+v\n", err)
		}
	}()

	runtime.GC()

	// always initialize tree with goleveldb as snapshotDB and memDB as recentDB
	snapDB := db.NewDB("test", "goleveldb", dirName)
	defer snapDB.Close()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	memSize := mem.Alloc
	//maxVersion := 0
	var keys [][]byte
	for i := 0; i < 100; i++ {
		keys = append(keys, randBytes(keyLen))
	}

	// reset timer after initialization logic
	b.ResetTimer()
	t, _ := prepareTree(b, snapDB, db.NewMemDB(), keepEvery, keepRecent, 5, keyLen, dataLen)

	// create 30000 versions
	for i := 0; i < 5000; i++ {
		// set 5 keys per version
		for j := 0; j < 5; j++ {
			index := rand.Int63n(100)
			t.Set(keys[index], randBytes(dataLen))
		}
		_, _, err := t.SaveVersion()
		if err != nil {
			b.Errorf("Can't save version %d: %v", i, err)
		}

		// Pause timer to garbage-collect and remeasure memory usage
		b.StopTimer()
		runtime.GC()
		runtime.ReadMemStats(&mem)
		// update memSize if it has increased after saveVersion
		if memSize < mem.Alloc {
			memSize = mem.Alloc
			//maxVersion = i
		}
		b.StartTimer()
	}

	//fmt.Printf("Maxmimum Memory usage was %0.2f MB at height %d\n", float64(memSize)/1000000, maxVersion)
	b.StopTimer()
}

func BenchmarkPruningStrategies(b *testing.B) {
	ps := []pruningstrat{
		{1, 0}, // default pruning strategy
		{1, 1},
		{0, 1}, // keep single recent version
		{100, 1},
		{100, 5}, // simple pruning
		{5, 1},
		{5, 2},
		{10, 2},
		{1000, 10},   // average pruning
		{1000, 1},    // extreme pruning
		{10000, 100}, // SDK pruning
	}
	for _, ps := range ps {
		ps := ps
		prefix := fmt.Sprintf("PruningStrategy{%d-%d}-KeyLen:%d-DataLen:%d", ps.keepEvery, ps.keepRecent, 16, 40)

		b.Run(prefix, func(sub *testing.B) {
			runBlockChain(sub, prefix, ps.keepEvery, ps.keepRecent, 16, 40)
		})
	}
}
