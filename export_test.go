package iavl

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

func TestExportImport(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0)
	require.NoError(t, err)

	tree.Set([]byte("x"), []byte{255})
	tree.Set([]byte("z"), []byte{255})
	tree.Set([]byte("a"), []byte{1})
	tree.Set([]byte("b"), []byte{2})
	tree.Set([]byte("c"), []byte{3})
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	tree.Remove([]byte("x"))
	tree.Remove([]byte("b"))
	tree.Set([]byte("c"), []byte{255})
	tree.Set([]byte("d"), []byte{4})
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	tree.Set([]byte("b"), []byte{2})
	tree.Set([]byte("c"), []byte{3})
	tree.Set([]byte("e"), []byte{5})
	tree.Remove([]byte("z"))
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	itree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	exporter := NewExporter(itree)

	newTree, err := NewMutableTree(db.NewMemDB(), 0)
	require.NoError(t, err)
	importer, err := NewImporter(newTree, version)
	require.NoError(t, err)

	for {
		item, err := exporter.Next()
		if err == io.EOF {
			err = importer.Done()
			require.NoError(t, err)
			break
		}
		require.NoError(t, err)
		err = importer.Import(item)
		require.NoError(t, err)
	}

	require.Equal(t, tree.Hash(), newTree.Hash())
	require.Equal(t, tree.Size(), newTree.Size())

	itree.Iterate(func(key, value []byte) bool {
		index, _ := tree.Get(key)
		newIndex, newValue := newTree.Get(key)
		require.Equal(t, index, newIndex)
		require.Equal(t, value, newValue)
		return false
	})
}
