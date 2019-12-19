package iavl

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	db "github.com/tendermint/tm-db"
)

func TestDelete(t *testing.T) {
	memDb := db.NewMemDB()
	tree, err := NewMutableTree(memDb, 0)
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
	memDb.Set(key, hash)
	tree.versions[version] = true

	k1Value, _, err = tree.GetVersionedWithProof([]byte("k1"), version)
	require.Nil(t, err)
	require.Equal(t, 0, bytes.Compare([]byte("Fred"), k1Value))
}

func TestTraverse(t *testing.T) {
	memDb := db.NewMemDB()
	tree, err := NewMutableTree(memDb, 0)
	require.NoError(t, err)

	for i := 0; i < 6; i++ {
		tree.set([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}

	require.Equal(t, 11, tree.nodeSize(), "Size of tree unexpected")
}

func TestEmptyRecents(t *testing.T) {
	memDB := db.NewMemDB()
	opts := Options{
		KeepRecent: 100,
		KeepEvery:  10000,
	}

	tree, err := NewMutableTreeWithOpts(memDB, db.NewMemDB(), 0, &opts)
	require.NoError(t, err)
	hash, version, err := tree.SaveVersion()

	require.Nil(t, err)
	require.Equal(t, int64(1), version)
	require.Nil(t, hash)
	require.True(t, tree.VersionExists(int64(1)))

	_, err = tree.GetImmutable(int64(1))
	require.Nil(t, err)
}

func BenchmarkMutableTree_Set(b *testing.B) {
	db := db.NewDB("test", db.MemDBBackend, "")
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
