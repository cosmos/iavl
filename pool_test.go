package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_TheLimitsOfMySanity(t *testing.T) {
	pool := NewNodePool()
	node := pool.Get()
	node.key = []byte("hello")
	n2 := pool.clone(node)
	require.Equal(t, node.key, n2.key)
	node.key = []byte("world")
	require.NotEqual(t, node.key, n2.key)
}
