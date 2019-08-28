package iavl

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNode_aminoSize(t *testing.T) {
	node := &Node{
		key:       randBytes(10),
		value:     randBytes(10),
		version:   1,
		height:    0,
		size:      100,
		hash:      randBytes(20),
		leftHash:  randBytes(20),
		leftNode:  nil,
		rightHash: randBytes(20),
		rightNode: nil,
		persisted: false,
	}

	// leaf node
	var buf bytes.Buffer
	err := node.writeBytes(&buf)
	require.NoError(t, err)
	require.Equal(t, buf.Len(), node.aminoSize())

	// non-leaf node
	node.height = 1
	buf.Reset()
	err = node.writeBytes(&buf)
	require.NoError(t, err)
	require.Equal(t, buf.Len(), node.aminoSize())
}

func BenchmarkNode_aminoSize(b *testing.B) {
	b.StopTimer()
	node := &Node{
		key:       randBytes(25),
		value:     randBytes(100),
		version:   rand.Int63n(10000000),
		height:    1,
		size:      rand.Int63n(10000000),
		leftHash:  randBytes(20),
		rightHash: randBytes(20),
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		node.aminoSize()
	}
}

func BenchmarkNode_WriteBytes(b *testing.B) {
	b.StopTimer()
	node := &Node{
		key:       randBytes(25),
		value:     randBytes(100),
		version:   rand.Int63n(10000000),
		height:    1,
		size:      rand.Int63n(10000000),
		leftHash:  randBytes(20),
		rightHash: randBytes(20),
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		buf.Reset()
		_ = node.writeBytes(&buf)
	}
}

func BenchmarkNode_WriteBytesPrealloc(b *testing.B) {
	b.StopTimer()
	node := &Node{
		key:       randBytes(25),
		value:     randBytes(100),
		version:   rand.Int63n(10000000),
		height:    1,
		size:      rand.Int63n(10000000),
		leftHash:  randBytes(20),
		rightHash: randBytes(20),
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		buf.Grow(node.aminoSize())
		_ = node.writeBytes(&buf)
	}
}
