package iavl

import (
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

// setupExportTreeBasic sets up a basic tree with a handful of
// create/update/delete operations over a few versions
func setupExportTreeBasic(t require.TestingT) *ImmutableTree {
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
	return itree
}

// setupExportTreeSized sets up a single-version tree with a given number
// of randomly generated key/value pairs
func setupExportTreeSized(t require.TestingT, treeSize int) *ImmutableTree {
	const (
		randSeed  = 49872768940 // For deterministic tests
		keySize   = 16
		valueSize = 16
	)

	r := rand.New(rand.NewSource(randSeed))
	tree, err := NewMutableTree(db.NewMemDB(), 0)
	require.NoError(t, err)

	for i := 0; i < treeSize; i++ {
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		r.Read(key)
		r.Read(value)
		updated := tree.Set(key, value)
		if updated {
			i--
		}
	}

	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	itree, err := tree.GetImmutable(version)
	require.NoError(t, err)

	return itree
}

func TestExportImport(t *testing.T) {
	tree := setupExportTreeBasic(t)
	exporter := NewExporter(tree)

	newTree, err := NewMutableTree(db.NewMemDB(), 0)
	require.NoError(t, err)
	importer, err := NewImporter(newTree, tree.Version())
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

	tree.Iterate(func(key, value []byte) bool {
		index, _ := tree.Get(key)
		newIndex, newValue := newTree.Get(key)
		require.Equal(t, index, newIndex)
		require.Equal(t, value, newValue)
		return false
	})
}

func BenchmarkExport(b *testing.B) {
	tree := setupExportTreeSized(b, 4096)
	for n := 0; n < b.N; n++ {
		exporter := NewExporter(tree)
		for {
			_, err := exporter.Next()
			if err == io.EOF {
				break
			}
			require.NoError(b, err)
		}
		exporter.Close()
	}
}

func BenchmarkImport(b *testing.B) {
	tree := setupExportTreeSized(b, 4096)
	exported := make([]ExportNode, 0, 4096)
	exporter := NewExporter(tree)
	for {
		item, err := exporter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(b, err)
		exported = append(exported, item)
	}
	exporter.Close()

	for n := 0; n < b.N; n++ {
		newTree, err := NewMutableTree(db.NewMemDB(), 0)
		require.NoError(b, err)
		importer, err := NewImporter(newTree, tree.Version())
		require.NoError(b, err)
		for _, item := range exported {
			err = importer.Import(item)
			require.NoError(b, err)
		}
		err = importer.Done()
		require.NoError(b, err)
	}
}
