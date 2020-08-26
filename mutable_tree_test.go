package iavl

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

func TestDelete(t *testing.T) {
	memDB := db.NewMemDB()
	tree, err := NewMutableTree(memDB, 0)
	require.NoError(t, err)

	tree.set([]byte("k1"), []byte("Fred"))
	hash, version, err := tree.SaveVersion()
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	require.NoError(t, tree.DeleteVersion(version))

	k1Value, _, _ := tree.GetVersionedWithProof([]byte("k1"), version)
	require.Nil(t, k1Value)

	key := tree.ndb.rootKey(version)
	err = memDB.Set(key, hash)
	require.NoError(t, err)
	tree.versions[version] = true

	k1Value, _, err = tree.GetVersionedWithProof([]byte("k1"), version)
	require.Nil(t, err)
	require.Equal(t, 0, bytes.Compare([]byte("Fred"), k1Value))
}

func TestTraverse(t *testing.T) {
	memDB := db.NewMemDB()
	tree, err := NewMutableTree(memDB, 0)
	require.NoError(t, err)

	for i := 0; i < 6; i++ {
		tree.set([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}

	require.Equal(t, 11, tree.nodeSize(), "Size of tree unexpected")
}

func TestMutableTree_DeleteVersions(t *testing.T) {
	memDB := db.NewMemDB()
	tree, err := NewMutableTree(memDB, 0)
	require.NoError(t, err)

	type entry struct {
		key   []byte
		value []byte
	}

	versionEntries := make(map[int64][]entry)

	// create 10 tree versions, each with 1000 random key/value entries
	for i := 0; i < 10; i++ {
		entries := make([]entry, 1000)

		for j := 0; j < 1000; j++ {
			k := randBytes(10)
			v := randBytes(10)

			entries[j] = entry{k, v}
			_ = tree.Set(k, v)
		}

		_, v, err := tree.SaveVersion()
		require.NoError(t, err)

		versionEntries[v] = entries
	}

	// delete even versions
	versionsToDelete := []int64{2, 4, 6, 8}
	require.NoError(t, tree.DeleteVersions(versionsToDelete...))

	// ensure even versions have been deleted
	for _, v := range versionsToDelete {
		require.False(t, tree.versions[v])

		_, err := tree.LazyLoadVersion(v)
		require.Error(t, err)
	}

	// ensure odd number versions exist and we can query for all set entries
	for _, v := range []int64{1, 3, 5, 7, 9, 10} {
		require.True(t, tree.versions[v])

		_, err := tree.LazyLoadVersion(v)
		require.NoError(t, err)

		for _, e := range versionEntries[v] {
			_, val := tree.Get(e.key)
			require.Equal(t, e.value, val)
		}
	}
}

func TestMutableTree_InitialVersion(t *testing.T) {
	memDB := db.NewMemDB()
	tree, err := NewMutableTreeWithOpts(memDB, 0, &Options{InitialVersion: 9})
	require.NoError(t, err)

	tree.Set([]byte("a"), []byte{0x01})
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 9, version)

	tree.Set([]byte("b"), []byte{0x02})
	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 10, version)

	// Reloading the tree with the same initial version is fine
	tree, err = NewMutableTreeWithOpts(memDB, 0, &Options{InitialVersion: 9})
	require.NoError(t, err)
	version, err = tree.Load()
	require.NoError(t, err)
	assert.EqualValues(t, 10, version)

	// Reloading the tree with an initial version beyond the lowest should error
	tree, err = NewMutableTreeWithOpts(memDB, 0, &Options{InitialVersion: 10})
	require.NoError(t, err)
	_, err = tree.Load()
	require.Error(t, err)

	// Reloading the tree with a lower initial version is fine, and new versions can be produced
	tree, err = NewMutableTreeWithOpts(memDB, 0, &Options{InitialVersion: 3})
	require.NoError(t, err)
	version, err = tree.Load()
	require.NoError(t, err)
	assert.EqualValues(t, 10, version)

	tree.Set([]byte("c"), []byte{0x03})
	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 11, version)
}

func TestMutableTree_SetInitialVersion(t *testing.T) {
	memDB := db.NewMemDB()
	tree, err := NewMutableTree(memDB, 0)
	require.NoError(t, err)
	tree.SetInitialVersion(9)

	tree.Set([]byte("a"), []byte{0x01})
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 9, version)
}

func BenchmarkMutableTree_Set(b *testing.B) {
	db, err := db.NewDB("test", db.MemDBBackend, "")
	require.NoError(b, err)
	t, err := NewMutableTree(db, 100000)
	require.NoError(b, err)
	for i := 0; i < 1000000; i++ {
		t.Set(randBytes(10), []byte{})
	}
	b.ReportAllocs()
	runtime.GC()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		t.Set(randBytes(10), []byte{})
	}
}
