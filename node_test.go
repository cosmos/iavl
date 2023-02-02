package iavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iavlrand "github.com/cosmos/iavl/internal/rand"
)

func TestNode_maxEncodedSize(t *testing.T) {
	node := &Node{
		key:           iavlrand.RandBytes(10),
		value:         iavlrand.RandBytes(10),
		version:       1,
		subtreeHeight: 0,
		size:          100,
		hash:          iavlrand.RandBytes(20),
		leftHash:      iavlrand.RandBytes(20),
		leftNode:      nil,
		rightHash:     iavlrand.RandBytes(20),
		rightNode:     nil,
		persisted:     false,
	}

	// leaf node
	require.Equal(t, 38, node.maxEncodedSize())

	// non-leaf node
	node.subtreeHeight = 1
	require.Equal(t, 92, node.maxEncodedSize())
}

func TestNode_encode_decode(t *testing.T) {
	testcases := map[string]struct {
		node        *Node
		expectHex   string
		expectError bool
	}{
		"nil":              {nil, "", true},
		"version overflow": {&Node{version: math.MaxUint32 + 1}, "", true},
		"empty":            {&Node{}, "000000000000", false},
		"inner": {&Node{
			subtreeHeight: 3,
			version:       2,
			size:          7,
			key:           []byte("key"),
			leftHash:      []byte{0x70, 0x80, 0x90, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			rightHash:     []byte{0x10, 0x20, 0x30, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		}, "0300020700036b6579708090a0000000000000000000000000000000000000000000000000000000001020304000000000000000000000000000000000000000000000000000000000", false},
		"leaf": {&Node{
			subtreeHeight: 0,
			version:       3,
			size:          1,
			key:           []byte("key"),
			value:         []byte("value"),
		}, "0000030100036b657976616c7565", false},
		"large number": {&Node{
			subtreeHeight: 0,
			version:       math.MaxUint32,
			size:          1 << 40,
			key:           []byte("key"),
			value:         []byte("value"),
		}, "0013ffffffff000001036b657976616c7565", false},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			bz, err := tc.node.Encode()
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectHex, hex.EncodeToString(bz))

			node, err := MakeNode(bz)
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
	h := []byte{1, 2, 3}
	c := &Node{key: []byte("child"), value: []byte("x"), version: 1, size: 1}

	testcases := map[string]struct {
		node  *Node
		valid bool
	}{
		"nil node":               {nil, false},
		"leaf":                   {&Node{key: k, value: v, version: 1, size: 1}, true},
		"leaf with nil key":      {&Node{key: nil, value: v, version: 1, size: 1}, false},
		"leaf with empty key":    {&Node{key: []byte{}, value: v, version: 1, size: 1}, true},
		"leaf with nil value":    {&Node{key: k, value: nil, version: 1, size: 1}, false},
		"leaf with empty value":  {&Node{key: k, value: []byte{}, version: 1, size: 1}, true},
		"leaf with version 0":    {&Node{key: k, value: v, version: 0, size: 1}, false},
		"leaf with version -1":   {&Node{key: k, value: v, version: -1, size: 1}, false},
		"leaf with size 0":       {&Node{key: k, value: v, version: 1, size: 0}, false},
		"leaf with size 2":       {&Node{key: k, value: v, version: 1, size: 2}, false},
		"leaf with size -1":      {&Node{key: k, value: v, version: 1, size: -1}, false},
		"leaf with left hash":    {&Node{key: k, value: v, version: 1, size: 1, leftHash: h}, false},
		"leaf with left child":   {&Node{key: k, value: v, version: 1, size: 1, leftNode: c}, false},
		"leaf with right hash":   {&Node{key: k, value: v, version: 1, size: 1, rightNode: c}, false},
		"leaf with right child":  {&Node{key: k, value: v, version: 1, size: 1, rightNode: c}, false},
		"inner":                  {&Node{key: k, version: 1, size: 1, subtreeHeight: 1, leftHash: h, rightHash: h}, true},
		"inner with nil key":     {&Node{key: nil, value: v, version: 1, size: 1, subtreeHeight: 1, leftHash: h, rightHash: h}, false},
		"inner with value":       {&Node{key: k, value: v, version: 1, size: 1, subtreeHeight: 1, leftHash: h, rightHash: h}, false},
		"inner with empty value": {&Node{key: k, value: []byte{}, version: 1, size: 1, subtreeHeight: 1, leftHash: h, rightHash: h}, false},
		"inner with left child":  {&Node{key: k, version: 1, size: 1, subtreeHeight: 1, leftHash: h}, true},
		"inner with right child": {&Node{key: k, version: 1, size: 1, subtreeHeight: 1, rightHash: h}, true},
		"inner with no child":    {&Node{key: k, version: 1, size: 1, subtreeHeight: 1}, false},
		"inner with height 0":    {&Node{key: k, version: 1, size: 1, subtreeHeight: 0, leftHash: h, rightHash: h}, false},
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

func BenchmarkNode_maxEncodedSize(b *testing.B) {
	// deterministic test case is good for comparison
	iavlrand.Seed(0)
	node := &Node{
		key:           iavlrand.RandBytes(25),
		value:         iavlrand.RandBytes(100),
		version:       rand.Int63n(10000000),
		subtreeHeight: 1,
		size:          rand.Int63n(10000000),
		leftHash:      iavlrand.RandBytes(20),
		rightHash:     iavlrand.RandBytes(20),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = node.maxEncodedSize()
	}
}

func BenchmarkNode_Encode(b *testing.B) {
	// deterministic test case is good for comparison
	iavlrand.Seed(0)
	node := &Node{
		key:           iavlrand.RandBytes(25),
		value:         iavlrand.RandBytes(100),
		version:       rand.Int63n(10000000),
		subtreeHeight: 1,
		size:          rand.Int63n(10000000),
		leftHash:      iavlrand.RandBytes(32),
		rightHash:     iavlrand.RandBytes(32),
	}
	largeNode := &Node{
		key:           iavlrand.RandBytes(25),
		value:         iavlrand.RandBytes(100),
		version:       rand.Int63n(10000000) + math.MaxUint16,
		subtreeHeight: 1,
		size:          rand.Int63n(10000000) + math.MaxUint32,
		leftHash:      iavlrand.RandBytes(32),
		rightHash:     iavlrand.RandBytes(32),
	}
	b.ResetTimer()
	b.Run("small integer", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			_, _ = node.Encode()
		}
	})
	b.Run("large integer", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			_, _ = largeNode.Encode()
		}
	})
}

func BenchmarkNode_Decode(b *testing.B) {
	// deterministic test case is good for comparison
	iavlrand.Seed(0)
	node := &Node{
		key:           iavlrand.RandBytes(25),
		value:         iavlrand.RandBytes(100),
		version:       rand.Int63n(10000000),
		subtreeHeight: 1,
		size:          rand.Int63n(10000000),
		leftHash:      iavlrand.RandBytes(32),
		rightHash:     iavlrand.RandBytes(32),
	}
	largeNode := &Node{
		key:           iavlrand.RandBytes(25),
		value:         iavlrand.RandBytes(100),
		version:       rand.Int63n(10000000) + math.MaxUint16,
		subtreeHeight: 1,
		size:          rand.Int63n(10000000) + math.MaxUint32,
		leftHash:      iavlrand.RandBytes(32),
		rightHash:     iavlrand.RandBytes(32),
	}

	buf, err := node.Encode()
	require.NoError(b, err)
	largeNodeBuf, err := largeNode.Encode()
	require.NoError(b, err)

	b.ResetTimer()
	b.Run("small number", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			_, _ = MakeNode(buf)
		}
	})
	b.Run("large number", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			_, _ = MakeNode(largeNodeBuf)
		}
	})
}

func BenchmarkNode_HashNode(b *testing.B) {
	node := &Node{
		key:           iavlrand.RandBytes(25),
		value:         iavlrand.RandBytes(100),
		version:       rand.Int63n(10000000),
		subtreeHeight: 1,
		size:          rand.Int63n(10000000),
		leftHash:      iavlrand.RandBytes(20),
		rightHash:     iavlrand.RandBytes(20),
	}
	b.ResetTimer()
	b.Run("NoBuffer", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			h := sha256.New()
			require.NoError(b, node.writeHashBytes(h))
			_ = h.Sum(nil)
		}
	})
	b.Run("PreAllocate", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			h := sha256.New()
			buf := new(bytes.Buffer)
			buf.Grow(node.maxEncodedSize())
			require.NoError(b, node.writeHashBytes(buf))
			_, err := h.Write(buf.Bytes())
			require.NoError(b, err)
			_ = h.Sum(nil)
		}
	})
	b.Run("NoPreAllocate", func(sub *testing.B) {
		sub.ReportAllocs()
		for i := 0; i < sub.N; i++ {
			h := sha256.New()
			buf := new(bytes.Buffer)
			require.NoError(b, node.writeHashBytes(buf))
			_, err := h.Write(buf.Bytes())
			require.NoError(b, err)
			_ = h.Sum(nil)
		}
	})
}
