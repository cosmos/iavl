package iavl

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

func TestFlushVersion(t *testing.T) {
	memDB := db.NewMemDB()
	opts := PruningOptions(5, 1)

	tree, err := NewMutableTreeWithOpts(memDB, db.NewMemDB(), 0, opts)
	require.NoError(t, err)
	require.NotNil(t, tree)

	// set key/value pairs and commit up to KeepEvery
	rootHashes := make([][]byte, 0)
	for i := int64(0); i < opts.KeepEvery; i++ {
		tree.Set([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))

		rh, v, err := tree.SaveVersion() // nolint: govet
		require.NoError(t, err)
		require.Equal(t, i+1, v)

		rootHashes = append(rootHashes, rh)
	}

	// verify the latest version
	require.Equal(t, int64(5), tree.Version())

	// verify we only have the 1st and KeepEvery version flushed to disk
	for i, rh := range rootHashes {
		version := int64(i + 1)

		ok, err := tree.ndb.HasSnapshot(rh) // nolint: govet
		require.NoError(t, err)

		if version == 1 || version%opts.KeepEvery == 0 {
			require.True(t, ok)
		} else {
			require.False(t, ok)
		}
	}

	// set key/value pairs and commit 2 more times (no flush to disk should occur)
	for i := opts.KeepEvery; i < opts.KeepEvery+2; i++ {
		tree.set([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))

		rh, v, err := tree.SaveVersion() // nolint: govet
		require.NoError(t, err)
		require.Equal(t, i+1, v)

		rootHashes = append(rootHashes, rh)
	}

	// verify the latest version
	require.Equal(t, int64(7), tree.Version())

	// verify we do not have the latest version flushed to disk
	ok, err := tree.ndb.HasSnapshot(rootHashes[len(rootHashes)-1])
	require.NoError(t, err)
	require.False(t, ok)

	// verify flushing already flushed version is fine
	require.NoError(t, tree.FlushVersion(5))

	// verify we can flush the latest version
	require.NoError(t, tree.FlushVersion(tree.Version()))

	// verify we do have the latest version flushed to disk
	ok, err = tree.ndb.HasSnapshot(rootHashes[len(rootHashes)-1])
	require.NoError(t, err)
	require.True(t, ok)

	tree2, err := NewMutableTreeWithOpts(memDB, db.NewMemDB(), 0, opts)
	require.NoError(t, err)
	require.NotNil(t, tree2)

	// verify we can load the previously manually flushed version on a new tree
	// and fetch all keys and values
	v, err := tree2.LoadVersion(tree.Version())
	require.NoError(t, err)
	require.Equal(t, tree.Version(), v)

	for i := int64(0); i < v; i++ {
		_, value := tree2.Get([]byte(fmt.Sprintf("key-%d", i)))
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}

	// also verify that we can load the automatically flushed version and fetch
	// all keys and values, and that no subsequent keys are present.
	v, err = tree2.LoadVersion(5)
	require.NoError(t, err)
	require.EqualValues(t, 5, v)

	for i := int64(0); i < v+10; i++ {
		_, value := tree2.Get([]byte(fmt.Sprintf("key-%d", i)))
		if i < v {
			assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
		} else {
			assert.Nil(t, value)
		}
	}
}

func TestFlushVersion_SetGet(t *testing.T) {
	memDB := db.NewMemDB()
	opts := PruningOptions(5, 1)

	tree, err := NewMutableTreeWithOpts(memDB, db.NewMemDB(), 0, opts)
	require.NoError(t, err)
	require.NotNil(t, tree)

	tree.Set([]byte("a"), []byte{1})
	tree.Set([]byte("b"), []byte{2})
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	err = tree.FlushVersion(version)
	require.NoError(t, err)

	tree, err = NewMutableTreeWithOpts(memDB, db.NewMemDB(), 0, opts)
	require.NoError(t, err)
	_, err = tree.LoadVersion(version)
	require.NoError(t, err)

	_, value := tree.Get([]byte("a"))
	assert.Equal(t, []byte{1}, value)
	_, value = tree.Get([]byte("b"))
	assert.Equal(t, []byte{2}, value)
}

func TestFlushVersion_Missing(t *testing.T) {
	tree, err := NewMutableTreeWithOpts(db.NewMemDB(), db.NewMemDB(), 0, PruningOptions(5, 1))
	require.NoError(t, err)
	require.NotNil(t, tree)

	tree.Set([]byte("a"), []byte{1})
	tree.Set([]byte("b"), []byte{2})
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	err = tree.FlushVersion(2)
	require.Error(t, err)
}

func TestFlushVersion_Empty(t *testing.T) {
	memDB := db.NewMemDB()
	opts := PruningOptions(5, 1)
	tree, err := NewMutableTreeWithOpts(memDB, db.NewMemDB(), 0, opts)
	require.NoError(t, err)
	require.NotNil(t, tree)

	// save a couple of versions
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 1, version)

	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 2, version)

	// flush the latest version
	err = tree.FlushVersion(2)
	require.NoError(t, err)

	// try to load the tree in a new memDB
	tree, err = NewMutableTreeWithOpts(memDB, db.NewMemDB(), 0, opts)
	require.NoError(t, err)
	require.NotNil(t, tree)

	version, err = tree.LoadVersion(2)
	require.NoError(t, err)
	assert.EqualValues(t, 2, version)

	// loading the first version should fail
	tree, err = NewMutableTreeWithOpts(memDB, db.NewMemDB(), 0, opts)
	require.NoError(t, err)
	require.NotNil(t, tree)

	_, err = tree.LoadVersion(1)
	require.Error(t, err)
}

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
	require.NoError(t, err)
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

func TestDeleteVersion_issue261(t *testing.T) {
	makeVersions := func(t *testing.T, mainDB db.DB, versions int, keepEvery int64) {
		// We set the same RNG for every run, which of course means we'll be regenerating the
		// same keys and values.
		r := rand.New(rand.NewSource(49872768940))

		// This follows SDK behavior.
		keepRecent := int64(1)
		if keepEvery == 1 {
			keepRecent = 0
		}

		// Create a new tree from the given database, with a fresh RecentDB for in-memory versions.
		// We'll enable sync, for good measure.
		tree, err := NewMutableTreeWithOpts(mainDB, db.NewMemDB(), 0, &Options{
			KeepEvery:  keepEvery,
			KeepRecent: keepRecent,
			Sync:       true,
		})
		require.NoError(t, err)

		// Load the latest persisted version.
		version, err := tree.LoadVersion(0)
		require.NoError(t, err)

		// Create new versions.
		for v := 0; v < versions; v++ {
			for i := 0; i < 4; i++ {
				key := []byte(fmt.Sprintf("%v", r.Intn(65536)))
				value := []byte(fmt.Sprintf("%v", r.Intn(1<<20)))
				tree.Set(key, value)
			}

			_, version, err = tree.SaveVersion()
			require.NoError(t, err)

			// Delete the previous keepEvery version if it's a multiple of KeepEvery. This follows
			// SDK pruning behavior.
			if version%keepEvery == 0 && version > keepEvery {
				err = tree.DeleteVersion(version - keepEvery)
				require.NoError(t, err)
			}
		}
	}

	// Use the same on-disk database for all runs.
	tempdir, err := ioutil.TempDir("", "iavl")
	require.NoError(t, err)
	defer os.RemoveAll(tempdir)

	levelDB, err := db.NewGoLevelDB("leveldb", tempdir)
	require.NoError(t, err)

	// First, create 9 versions and persist every 4 versions to disk, deleting the previous
	// persisted version.
	makeVersions(t, levelDB, 9, 4)

	// Now, create another 9 versions and persist every 4 versions to disk, deleting the previous
	// persisted one. This will load version 8 (the last persisted one) and generate new versions
	// from that. In the original issue, this panics after version 8 has been deleted, when version
	// 9 is about to be created.
	makeVersions(t, levelDB, 9, 4)
}

func TestDeleteVersion_issue261_minimal(t *testing.T) {
	// Use the same on-disk database for all runs.
	tempdir, err := ioutil.TempDir("", "iavl")
	require.NoError(t, err)
	defer os.RemoveAll(tempdir)

	levelDB, err := db.NewGoLevelDB("leveldb", tempdir)
	require.NoError(t, err)

	// Create a new tree that persists data to disk every 2 versions. KeepRecent: 1 mirrors
	// SDK behavior.
	tree, err := NewMutableTreeWithOpts(levelDB, db.NewMemDB(), 0, &Options{
		KeepEvery:  2,
		KeepRecent: 1,
	})
	require.NoError(t, err)
	version, err := tree.Load()
	require.NoError(t, err)
	require.EqualValues(t, 0, version)

	// Write two versions, where the second is persisted to disk.
	tree.Set([]byte{1}, []byte{1})
	tree.Set([]byte{2}, []byte{2})
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	tree.Set([]byte{3}, []byte{3})
	tree.Set([]byte{4}, []byte{4})
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	// Now, make a single version that is only persisted in memory. This will cause the previous
	// node to become orphaned, and the orphaning incorrectly persisted to disk.
	tree.Set([]byte{4}, []byte{0})
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	// Close the tree, and set up a new one which loads the persisted version 2.
	tree, err = NewMutableTreeWithOpts(levelDB, db.NewMemDB(), 0, &Options{
		KeepEvery:  2,
		KeepRecent: 1,
	})
	require.NoError(t, err)
	version, err = tree.Load()
	require.NoError(t, err)
	require.EqualValues(t, 2, version)

	// If we now write two other versions, which do not touch the branch of key 4, and then
	// delete version 2 (following SDK behavior), this will cause key 4 to go missing because the
	// incorrect orphan written during in-memory version 3 has scheduled it for deletion, even
	// though the change that caused it to be orphaned now hasn't happened.
	tree.Set([]byte{1}, []byte{0})
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	tree.Set([]byte{2}, []byte{0})
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	err = tree.DeleteVersion(2)
	require.NoError(t, err)

	_, v := tree.Get([]byte{4})
	assert.Equal(t, []byte{4}, v)
}
