package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodePool_Get(t *testing.T) {
	pool := NewNodePool()
	node := pool.Get()
	require.Equal(t, []byte(nil), node.key)
	node.key = []byte("hello")
	pool.Put(node)
	require.Equal(t, []byte(nil), pool.Get().key)
}
