package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodePool_Get(t *testing.T) {
	pool := NewNodePool()
	node := pool.Get()
	node.key = []byte("hello")
	require.Equal(t, node.key, pool.nodes[node.poolID].key)
	pool.Put(node)
	require.Equal(t, []byte(nil), pool.nodes[node.poolID].key)
}
