package iavl

import (
	"io"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

// setupExportTreeBasic sets up a basic tree with a handful of
// create/update/delete operations over a few versions.
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

// setupExportTreeRandom sets up a randomly generated tree.
func setupExportTreeRandom(t *testing.T) *ImmutableTree {
	const (
		randSeed  = 49872768940 // For deterministic tests
		keySize   = 16
		valueSize = 16

		versions    = 32   // number of versions to generate
		versionOps  = 4096 // number of operations (create/update/delete) per version
		updateRatio = 0.4  // ratio of updates out of all operations
		deleteRatio = 0.2  // ratio of deletes out of all operations
	)

	r := rand.New(rand.NewSource(randSeed))
	tree, err := NewMutableTree(db.NewMemDB(), 0)
	require.NoError(t, err)

	var version int64
	keys := make([][]byte, 0, versionOps)
	for i := 0; i < versions; i++ {
		for j := 0; j < versionOps; j++ {
			key := make([]byte, keySize)
			value := make([]byte, valueSize)

			// The performance of this is likely to be terrible, but that's fine for small tests
			switch {
			case len(keys) > 0 && r.Float64() <= deleteRatio:
				index := r.Intn(len(keys))
				key = keys[index]
				keys = append(keys[:index], keys[index+1:]...)
				_, removed := tree.Remove(key)
				require.True(t, removed)

			case len(keys) > 0 && r.Float64() <= updateRatio:
				key = keys[r.Intn(len(keys))]
				r.Read(value)
				updated := tree.Set(key, value)
				require.True(t, updated)

			default:
				r.Read(key)
				r.Read(value)
				// if we get an update, set again
				for tree.Set(key, value) {
					r.Read(key)
				}
				keys = append(keys, key)
			}
		}
		_, version, err = tree.SaveVersion()
		require.NoError(t, err)
	}

	require.EqualValues(t, versions, tree.Version())
	require.GreaterOrEqual(t, tree.Size(), int64(math.Trunc(versions*versionOps*(1-updateRatio-deleteRatio))/2))

	itree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	return itree
}

// setupExportTreeSized sets up a single-version tree with a given number
// of randomly generated key/value pairs, useful for benchmarking.
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
	testcases := map[string]struct {
		tree *ImmutableTree
	}{
		"empty tree":  {tree: NewImmutableTree(db.NewMemDB(), 0)},
		"basic tree":  {tree: setupExportTreeBasic(t)},
		"sized tree":  {tree: setupExportTreeSized(t, 4096)},
		"random tree": {tree: setupExportTreeRandom(t)},
	}
	for desc, tc := range testcases {
		tc := tc // appease scopelint
		t.Run(desc, func(t *testing.T) {
			exporter := NewExporter(tc.tree)

			newTree, err := NewMutableTree(db.NewMemDB(), 0)
			require.NoError(t, err)
			importer, err := NewImporter(newTree, tc.tree.Version())
			require.NoError(t, err)

			for {
				item, err := exporter.Next()
				if err == io.EOF {
					err = importer.Commit()
					require.NoError(t, err)
					break
				}
				require.NoError(t, err)
				err = importer.Add(item)
				require.NoError(t, err)
			}

			require.Equal(t, tc.tree.Hash(), newTree.Hash(), "Tree hash mismatch")
			require.Equal(t, tc.tree.Size(), newTree.Size(), "Tree size mismatch")
			require.Equal(t, tc.tree.Version(), newTree.Version(), "Tree version mismatch")

			tc.tree.Iterate(func(key, value []byte) bool {
				index, _ := tc.tree.Get(key)
				newIndex, newValue := newTree.Get(key)
				require.Equal(t, index, newIndex, "Index mismatch for key %v", key)
				require.Equal(t, value, newValue, "Value mismatch for key %v", key)
				return false
			})

		})
	}
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
