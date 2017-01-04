package benchmarks

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"

	db "github.com/tendermint/go-db"
	merkle "github.com/tendermint/go-merkle"
)

func randBytes(length int) []byte {
	key := make([]byte, length)
	// math.rand.Read always returns err=nil
	rand.Read(key)
	return key
}

func prepareTree(db db.DB, size, keyLen, dataLen int) (merkle.Tree, [][]byte) {
	t := merkle.NewIAVLTree(size, db)
	keys := make([][]byte, size)

	for i := 0; i < size; i++ {
		key := randBytes(keyLen)
		t.Set(key, randBytes(dataLen))
		keys[i] = key
	}
	t.Save()
	runtime.GC()
	return t, keys
}

func runQueries(b *testing.B, t merkle.Tree, keyLen int) {
	for i := 0; i < b.N; i++ {
		q := randBytes(keyLen)
		t.Get(q)
	}
}

func runKnownQueries(b *testing.B, t merkle.Tree, keys [][]byte) {
	l := int32(len(keys))
	for i := 0; i < b.N; i++ {
		q := keys[rand.Int31n(l)]
		t.Get(q)
	}
}

func runInsert(b *testing.B, t merkle.Tree, keyLen, dataLen, blockSize int) {
	for i := 1; i <= b.N; i++ {
		t.Set(randBytes(keyLen), randBytes(dataLen))
		if i%blockSize == 0 {
			t.Hash()
			t.Save()
		}
	}
}

func runUpdate(b *testing.B, t merkle.Tree, dataLen, blockSize int, keys [][]byte) {
	l := int32(len(keys))
	for i := 1; i <= b.N; i++ {
		key := keys[rand.Int31n(l)]
		t.Set(key, randBytes(dataLen))
		if i%blockSize == 0 {
			t.Hash()
			t.Save()
		}
	}
}

func runDelete(b *testing.B, t merkle.Tree, blockSize int, keys [][]byte) {
	l := int32(len(keys))
	for i := 1; i <= b.N; i++ {
		key := keys[rand.Int31n(l)]
		// TODO: test if removed, use more keys (from insert)
		t.Remove(key)
		if i%blockSize == 0 {
			t.Hash()
			t.Save()
		}
	}
}

func runTMSP(b *testing.B, t merkle.Tree, keyLen, dataLen, blockSize int, keys [][]byte) {
	l := int32(len(keys))

	lastCommit := t
	real := t.Copy()
	check := t.Copy()

	for i := 1; i <= b.N; i++ {
		// 50% insert, 50% update
		var key []byte
		if i%2 == 0 {
			key = keys[rand.Int31n(l)]
		} else {
			key = randBytes(keyLen)
		}
		data := randBytes(dataLen)

		// perform query and write on check and then real
		check.Get(key)
		check.Set(key, data)
		real.Get(key)
		real.Set(key, data)

		// at the end of a block, move it all along....
		if i%blockSize == 0 {
			real.Hash()
			real.Save()
			lastCommit = real
			real = lastCommit.Copy()
			check = lastCommit.Copy()
		}
	}
}

func bbenchmarkRandomBytes(b *testing.B) {
	benchmarks := []struct {
		length int
	}{
		{4}, {16}, {32},
	}
	for _, bench := range benchmarks {
		name := fmt.Sprintf("random-%d", bench.length)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				randBytes(bench.length)
			}
			runtime.GC()
		})
	}
}

func BenchmarkAllTrees(b *testing.B) {
	benchmarks := []struct {
		dbType              string
		initSize, blockSize int
		keyLen, dataLen     int
	}{
		{"", 100000, 100, 16, 40},
		{"memdb", 100000, 100, 16, 40},
		{"goleveldb", 100000, 100, 16, 40},
		// FIXME: this crashes on init!
		// {"cleveldb", 100000, 100, 16, 40},
		{"leveldb", 100000, 100, 16, 40},
	}

	for _, bb := range benchmarks {
		name := bb.dbType
		if name == "" {
			name = "nodb"
		}
		prefix := fmt.Sprintf("%s-%d-%d-%d-%d-", name, bb.initSize,
			bb.blockSize, bb.keyLen, bb.dataLen)

		// prepare a dir for the db and cleanup afterwards
		dirName := fmt.Sprintf("./%s-db", prefix)
		defer func() {
			err := os.RemoveAll(dirName)
			if err != nil {
				fmt.Printf("%+v\n", err)
			}
		}()

		// note that "" leads to nil backing db!
		var d db.DB
		if bb.dbType != "" {
			d = db.NewDB("test", bb.dbType, dirName)
			defer d.Close()
		}
		runSuite(b, prefix, d, bb.initSize, bb.blockSize, bb.keyLen, bb.dataLen)
	}
}

func runSuite(b *testing.B, prefix string, d db.DB, initSize, blockSize, keyLen, dataLen int) {
	// setup code
	t, keys := prepareTree(d, initSize, keyLen, dataLen)
	b.ResetTimer()

	b.Run(prefix+"query-miss", func(b *testing.B) {
		runQueries(b, t, keyLen)
	})
	b.Run(prefix+"query-hits", func(b *testing.B) {
		runKnownQueries(b, t, keys)
	})
	b.Run(prefix+"update", func(b *testing.B) {
		runUpdate(b, t, dataLen, blockSize, keys)
	})
	b.Run(prefix+"insert", func(b *testing.B) {
		runInsert(b, t, keyLen, dataLen, blockSize)
	})

	// FIXME: this consistently causes a panic, but it doesn't show up in a simple test....
	// needs more investigation
	// b.Run(prefix+"delete", func(b *testing.B) {
	// 	runDelete(b, t, blockSize, keys)
	// })
	// b.Run(prefix+"tmsp", func(b *testing.B) {
	// 	runTMSP(b, t, keyLen, dataLen, blockSize, keys)
	// })

}
