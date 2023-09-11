package v6_test

import (
	"encoding/binary"
	"testing"
)

type nodeKey [12]byte

type node struct {
	nk  *nodeKey
	lnk *nodeKey
	rnk *nodeKey
}

func (nk *nodeKey) CopyZero() {
	*nk = nodeKey{}
}

func (nk *nodeKey) NoAllocZero() {
	for i := 0; i < 12; i++ {
		nk[i] = 0
	}
}

func Benchmark_NodeKey_CopyZero(b *testing.B) {
	for i := 0; i < b.N; i++ {
		nk := new(nodeKey)
		nk.CopyZero()
	}
}

func Benchmark_NodeKey_NoAllocZero(b *testing.B) {
	for i := 0; i < b.N; i++ {
		nk := new(nodeKey)
		nk.NoAllocZero()
	}
}

func Benchmark_NodeKey_AllocNew(b *testing.B) {
	var seq uint32
	for i := 0; i < b.N; i++ {
		nk := new(nodeKey)
		binary.BigEndian.PutUint64(nk[:], uint64(i))
		binary.BigEndian.PutUint32(nk[8:], seq)
		seq++
	}
}

func Benchmark_NodeKey_Overwrite(b *testing.B) {
	nk := new(nodeKey)
	var seq uint32
	for i := 0; i < b.N; i++ {
		nk.CopyZero()
		binary.BigEndian.PutUint64(nk[:], uint64(i))
		binary.BigEndian.PutUint32(nk[8:], seq)
		seq++
	}
}
