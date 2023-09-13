package iavl_test

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	"github.com/cosmos/iavl/v2"
	"github.com/stretchr/testify/require"
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

func Benchmark_NodeKey_StdCompare(b *testing.B) {
	emptyNk := nodeKey{}
	nk := nodeKey{}
	for i := 0; i < b.N; i++ {
		//j := byte(i%255) + 1
		//nk := nodeKey{0, 0, 0, 0, 0, 0, 0, j, j, j, j, j}
		_, err := rand.Read(nk[:])
		if err != nil {
			b.Fail()
		}
		if emptyNk == nk {
			panic("equal")
		}
	}
}

func Benchmark_NodeKey_FastCompare(b *testing.B) {
	emptyNk := nodeKey{}
	nk := nodeKey{}
	for i := 0; i < b.N; i++ {
		//j := byte(i%255) + 1
		//nk := nodeKey{0, 0, 0, 0, 0, 0, 0, j, j, j, j, j}
		_, err := rand.Read(nk[:])
		if err != nil {
			b.Fail()
		}
		if fastCompare(emptyNk, nk) {
			panic("equal")
		}
	}
}

func fastCompare(a nodeKey, b nodeKey) bool {
	for i := 0; i < 12; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func Test_ReadWriteNode(t *testing.T) {
	nk := iavl.NewNodeKey(101, 777)
	n := &iavl.Node{
		Key:           []byte("key"),
		NodeKey:       nk,
		LeftNodeKey:   iavl.NewNodeKey(101, 778),
		RightNodeKey:  iavl.NewNodeKey(101, 779),
		Size:          100_000,
		SubtreeHeight: 1,
	}
	bz := &bytes.Buffer{}
	err := n.WriteBytes(bz)
	require.NoError(t, err)
	n2, err := iavl.MakeNode(nk[:], bz.Bytes())
	require.NoError(t, err)
	require.Equal(t, n.Key, n2.Key)
	require.Equal(t, n.NodeKey, n2.NodeKey)
	require.Equal(t, n.LeftNodeKey, n2.LeftNodeKey)
	require.Equal(t, n.RightNodeKey, n2.RightNodeKey)
	require.Equal(t, n.Size, n2.Size)
	require.Equal(t, n.SubtreeHeight, n2.SubtreeHeight)

	// leaf node
	n.Value = []byte("value")
	n.SubtreeHeight = 0
	n.LeftNodeKey = iavl.NodeKey{}
	n.RightNodeKey = iavl.NodeKey{}
	bz = &bytes.Buffer{}
	err = n.WriteBytes(bz)
	require.NoError(t, err)
	n2, err = iavl.MakeNode(nk[:], bz.Bytes())
	require.NoError(t, err)
	require.Equal(t, n.Key, n2.Key)
	require.Equal(t, n.Value, n2.Value)
	require.Equal(t, n.NodeKey, n2.NodeKey)
	require.Equal(t, n.LeftNodeKey, n2.LeftNodeKey)
	require.Equal(t, n.RightNodeKey, n2.RightNodeKey)
	require.Equal(t, n.Size, n2.Size)
	require.Equal(t, n.SubtreeHeight, n2.SubtreeHeight)
}

func Test_NodeSize(t *testing.T) {
	n1 := struct {
		Key []byte
	}{}
	fmt.Printf("n1: %d\n", unsafe.Sizeof(n1))
}
