package iavl

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iavlrand "github.com/cosmos/iavl/internal/rand"
)

func TestNode_encodedSize(t *testing.T) {
	node := &Node{
		key:           iavlrand.RandBytes(10),
		value:         iavlrand.RandBytes(10),
		version:       1,
		subtreeHeight: 0,
		size:          100,
		hash:          iavlrand.RandBytes(20),
		nodeKey:       10,
		leftNodeKey:   20,
		leftNode:      nil,
		rightNodeKey:  30,
		rightNode:     nil,
		persisted:     false,
	}

	// leaf node
	require.Equal(t, 26, node.encodedSize())

	// non-leaf node
	node.subtreeHeight = 1
	require.Equal(t, 19, node.encodedSize())
}

func TestNode_encode_decode(t *testing.T) {
	testcases := map[string]struct {
		node        *Node
		expectHex   string
		expectError bool
	}{
		"nil":   {nil, "", true},
		"empty": {&Node{}, "0000000000", false},
		"inner": {&Node{
			subtreeHeight: 3,
			version:       2,
			size:          7,
			key:           []byte("key"),
			leftHash:      []byte{0x70, 0x80, 0x90, 0xa0},
			rightHash:     []byte{0x10, 0x20, 0x30, 0x40},
			nodeKey:       1,
			leftNodeKey:   2,
			rightNodeKey:  3,
		}, "060e04036b65790404708090a0060410203040", false},
		"leaf": {&Node{
			subtreeHeight: 0,
			version:       3,
			size:          1,
			key:           []byte("key"),
			value:         []byte("value"),
			nodeKey:       4,
		}, "000206036b65790576616c7565", false},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := tc.node.writeBytes(&buf)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectHex, hex.EncodeToString(buf.Bytes()))

			node, err := MakeNode(tc.node.nodeKey, buf.Bytes())
			require.NoError(t, err)
			// since key and value is always decoded to []byte{} we augment the expected struct here
			if tc.node.key == nil {
				tc.node.key = []byte{}
			}
			if tc.node.value == nil && tc.node.subtreeHeight == 0 {
				tc.node.value = []byte{}
			}
			require.Equal(t, tc.node, node)
		})
	}
}

func TestNode_validate(t *testing.T) {
	k := []byte("key")
	v := []byte("value")
	nk := int64(10)
	c := &Node{key: []byte("child"), value: []byte("x"), version: 1, size: 1}

	testcases := map[string]struct {
		node  *Node
		valid bool
	}{
		"nil node":                 {nil, false},
		"leaf":                     {&Node{key: k, value: v, version: 1, size: 1}, true},
		"leaf with nil key":        {&Node{key: nil, value: v, version: 1, size: 1}, false},
		"leaf with empty key":      {&Node{key: []byte{}, value: v, version: 1, size: 1}, true},
		"leaf with nil value":      {&Node{key: k, value: nil, version: 1, size: 1}, false},
		"leaf with empty value":    {&Node{key: k, value: []byte{}, version: 1, size: 1}, true},
		"leaf with version 0":      {&Node{key: k, value: v, version: 0, size: 1}, false},
		"leaf with version -1":     {&Node{key: k, value: v, version: -1, size: 1}, false},
		"leaf with size 0":         {&Node{key: k, value: v, version: 1, size: 0}, false},
		"leaf with size 2":         {&Node{key: k, value: v, version: 1, size: 2}, false},
		"leaf with size -1":        {&Node{key: k, value: v, version: 1, size: -1}, false},
		"leaf with left node key":  {&Node{key: k, value: v, version: 1, size: 1, leftNodeKey: nk}, false},
		"leaf with left child":     {&Node{key: k, value: v, version: 1, size: 1, leftNode: c}, false},
		"leaf with right node key": {&Node{key: k, value: v, version: 1, size: 1, rightNodeKey: nk}, false},
		"leaf with right child":    {&Node{key: k, value: v, version: 1, size: 1, rightNode: c}, false},
		"inner":                    {&Node{key: k, version: 1, size: 1, subtreeHeight: 1, leftNodeKey: nk, rightNodeKey: nk}, true},
		"inner with nil key":       {&Node{key: nil, value: v, version: 1, size: 1, subtreeHeight: 1, leftNodeKey: nk, rightNodeKey: nk}, false},
		"inner with value":         {&Node{key: k, value: v, version: 1, size: 1, subtreeHeight: 1, leftNodeKey: nk, rightNodeKey: nk}, false},
		"inner with empty value":   {&Node{key: k, value: []byte{}, version: 1, size: 1, subtreeHeight: 1, leftNodeKey: nk, rightNodeKey: nk}, false},
		"inner with left child":    {&Node{key: k, version: 1, size: 1, subtreeHeight: 1, leftNodeKey: nk}, true},
		"inner with right child":   {&Node{key: k, version: 1, size: 1, subtreeHeight: 1, rightNodeKey: nk}, true},
		"inner with no child":      {&Node{key: k, version: 1, size: 1, subtreeHeight: 1}, false},
		"inner with height 0":      {&Node{key: k, version: 1, size: 1, subtreeHeight: 0, leftNodeKey: nk, rightNodeKey: nk}, false},
	}

	for desc, tc := range testcases {
		tc := tc // appease scopelint
		t.Run(desc, func(t *testing.T) {
			err := tc.node.validate()
			if tc.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func BenchmarkNode_encodedSize(b *testing.B) {
	node := &Node{
		key:           iavlrand.RandBytes(25),
		value:         iavlrand.RandBytes(100),
		version:       rand.Int63n(10000000),
		subtreeHeight: 1,
		size:          rand.Int63n(10000000),
		leftNodeKey:   rand.Int63n(10000000),
		rightNodeKey:  rand.Int63n(10000000),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node.encodedSize()
	}
}

func BenchmarkNode_WriteBytes(b *testing.B) {
	node := &Node{
		key:           iavlrand.RandBytes(25),
		value:         iavlrand.RandBytes(100),
		version:       rand.Int63n(10000000),
		subtreeHeight: 1,
		size:          rand.Int63n(10000000),
		leftNodeKey:   rand.Int63n(10000000),
		rightNodeKey:  rand.Int63n(10000000),
	}
	b.ResetTimer()
	b.Run("NoPreAllocate", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			var buf bytes.Buffer
			_ = node.writeBytes(&buf)
		}
	})
	b.Run("PreAllocate", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			var buf bytes.Buffer
			buf.Grow(node.encodedSize())
			_ = node.writeBytes(&buf)
		}
	})
}
