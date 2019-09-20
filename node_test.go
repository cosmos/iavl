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
	require.Equal(t, 26, node.aminoSize())

	// non-leaf node
	node.height = 1
	require.Equal(t, 57, node.aminoSize())
}

func BenchmarkNode_aminoSize(b *testing.B) {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node.aminoSize()
	}
}

func BenchmarkNode_WriteBytes(b *testing.B) {
	node := &Node{
		key:       randBytes(25),
		value:     randBytes(100),
		version:   rand.Int63n(10000000),
		height:    1,
		size:      rand.Int63n(10000000),
		leftHash:  randBytes(20),
		rightHash: randBytes(20),
	}
	b.ResetTimer()
	b.Run("NoPreAllocate", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			var buf bytes.Buffer
			buf.Reset()
			_ = node.writeBytes(&buf)
		}
	})
	b.Run("PreAllocate", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			var buf bytes.Buffer
			buf.Grow(node.aminoSize())
			_ = node.writeBytes(&buf)
		}
	})
}
