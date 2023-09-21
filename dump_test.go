package iavl

import (
	"os"
	"testing"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/stretchr/testify/require"
)

func TestDumpNode(t *testing.T) {
	dumper := &dumper{}
	node := &Node{
		key:           []byte("1234"),
		nodeKey:       GetNodeKey([]byte("123456789012")),
		leftNodeKey:   []byte("123456789012"),
		rightNodeKey:  []byte("123456789012"),
		hash:          []byte("123456789012"),
		size:          1234,
		subtreeHeight: int8(123),
	}
	dump, err := dumper.MarshalNode(node)
	require.NoError(t, err)
	require.Equal(t, []byte("123456789012"), dump[:12])

	readNode, err := dumper.ReadNode(dump)
	require.NoError(t, err)
	require.Equal(t, node.nodeKey, readNode.nodeKey)
	require.Equal(t, node.leftNodeKey, readNode.leftNodeKey)
	require.Equal(t, node.rightNodeKey, readNode.rightNodeKey)
}

func TestDumpNodes(t *testing.T) {
	gen := bench.ChangesetGenerator{
		Seed:             77,
		KeyMean:          60,
		KeyStdDev:        5,
		ValueMean:        10,
		ValueStdDev:      2,
		InitialSize:      1000,
		FinalSize:        2000,
		Versions:         10,
		ChangePerVersion: 10,
		DeleteFraction:   0.1,
	}
	itr, err := gen.Iterator()
	require.NoError(t, err)

	nodesFile, err := os.Create("/tmp/nodes.dump")
	require.NoError(t, err)
	keysFiles, err := os.Create("/tmp/keys.dump")
	require.NoError(t, err)

	dumper := newDumper(nodesFile, keysFiles)
	require.NotNil(t, dumper)

	tree := &Tree{}

	for itr.Valid() {
		require.NoError(t, err)
		nodes := itr.Nodes()
		for ; nodes.Valid(); err = nodes.Next() {
			node := nodes.GetNode()
			require.False(t, node.Delete)
			_, err := tree.Set(node.Key, node.Value)
			require.NoError(t, err)
		}
		break
	}
}
