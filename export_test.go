package iavl

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

func TestExporter(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0)
	require.NoError(t, err)

	tree.Set([]byte("x"), []byte{255})
	tree.Set([]byte("z"), []byte{255})
	tree.Set([]byte("a"), []byte{1})
	tree.Set([]byte("b"), []byte{2})
	tree.Set([]byte("c"), []byte{3})
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	tree.Remove([]byte("x"))
	tree.Remove([]byte("b"))
	tree.Set([]byte("c"), []byte{255})
	tree.Set([]byte("d"), []byte{4})
	_, version, err = tree.SaveVersion()
	require.NoError(t, err)

	tree.Set([]byte("b"), []byte{2})
	tree.Set([]byte("c"), []byte{3})
	tree.Set([]byte("e"), []byte{5})
	tree.Remove([]byte("z"))
	_, version, err = tree.SaveVersion()

	itree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	exporter := NewExporter(context.Background(), itree)

	expect := [][]byte{
		[]byte("d"),
		[]byte("c"),
		[]byte("b"),
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("e"),
		[]byte("d"),
		[]byte("e"),
	}
	for _, e := range expect {
		n, err := exporter.Next()
		require.NoError(t, err)
		assert.Equal(t, ExportedNode{Key: e}, n)
	}
	n, err := exporter.Next()
	assert.Equal(t, io.EOF, err)
	assert.Empty(t, n)
}
