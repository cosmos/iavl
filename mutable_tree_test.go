package iavl

import (
	"runtime"
	"testing"

	db "github.com/tendermint/tm-db"
)

func BenchmarkMutableTree_Set(b *testing.B) {
	db := db.NewDB("test", db.MemDBBackend, "")
	t := NewMutableTree(db, 100000)
	for i := 0; i < 1000000; i++ {
		t.Set(randBytes(10), []byte{})
	}
	b.ReportAllocs()
	runtime.GC()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		t.Set(randBytes(10), []byte{})
	}
}
