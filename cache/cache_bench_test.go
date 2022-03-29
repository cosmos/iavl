package cache_test

import (
	"math/rand"
	"testing"

	"github.com/cosmos/iavl/cache"
)

func BenchmarkAdd(b *testing.B) {
	b.ReportAllocs()
	testcases := map[string]struct {
		cacheLimit int
		keySize    int
	}{
		"small - limit: 10K, key size - 10b": {
			cacheLimit: 10000,
			keySize:    10,
		},
		"med - limit: 100K, key size 20b": {
			cacheLimit: 100000,
			keySize:    20,
		},
		"large - limit: 1M, key size 30b": {
			cacheLimit: 1000000,
			keySize:    30,
		},
	}

	for name, tc := range testcases {
		cache := cache.New(tc.cacheLimit)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = cache.Add(&testNode{
					key: randBytes(tc.keySize),
				})
			}
		})
	}
}

func BenchmarkRemove(b *testing.B) {
	b.ReportAllocs()

	b.StopTimer()
	cache := cache.New(1000)
	existentKeyMirror := [][]byte{}
	// Populate cache
	for i := 0; i < 50; i++ {
		key := randBytes(1000)

		existentKeyMirror = append(existentKeyMirror, key)

		cache.Add(&testNode{
			key: key,
		})
	}

	randSeed := 49872768940 // For deterministic tests
	r := rand.New(rand.NewSource(int64(randSeed)))

	for i := 0; i < b.N; i++ {
		key := existentKeyMirror[r.Intn(len(existentKeyMirror))]
		b.ResetTimer()
		_ = cache.Remove(key)
	}
}
