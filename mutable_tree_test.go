package iavl

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"cosmossdk.io/core/log"

	iavlrand "github.com/cosmos/iavl/internal/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
)

var (
	tKey1 = []byte("k1")
	tVal1 = []byte("v1")

	tKey2 = []byte("k2")
	tVal2 = []byte("v2")
	// FIXME: enlarge maxIterator to 100000
	maxIterator = 100
)

func setupMutableTree() *MutableTree {
	memDB := dbm.NewMemDB()
	tree := NewMutableTree(memDB, 0, log.NewNopLogger())
	return tree
}

// TestIterateConcurrency throws "fatal error: concurrent map writes" when fast node is enabled
func TestIterateConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	tree := setupMutableTree()
	wg := new(sync.WaitGroup)
	for i := 0; i < 100; i++ {
		for j := 0; j < maxIterator; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				_, err := tree.Set([]byte(fmt.Sprintf("%d%d", i, j)), iavlrand.RandBytes(1))
				require.NoError(t, err)
			}(i, j)
		}
		tree.Iterate(func(_, _ []byte) bool { //nolint:errcheck
			return false
		})
	}
	wg.Wait()
}

// TestConcurrency throws "fatal error: concurrent map iteration and map write" and
// also sometimes "fatal error: concurrent map writes" when fast node is enabled
func TestIteratorConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	tree := setupMutableTree()
	_, err := tree.LoadVersion(0)
	require.NoError(t, err)
	// So much slower
	wg := new(sync.WaitGroup)
	for i := 0; i < 100; i++ {
		for j := 0; j < maxIterator; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				_, err := tree.Set([]byte(fmt.Sprintf("%d%d", i, j)), iavlrand.RandBytes(1))
				require.NoError(t, err)
			}(i, j)
		}
		itr, _ := tree.Iterator(nil, nil, true)
		for ; itr.Valid(); itr.Next() { //nolint:revive
		} // do nothing
	}
	wg.Wait()
}

// TestNewIteratorConcurrency throws "fatal error: concurrent map writes" when fast node is enabled
func TestNewIteratorConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	tree := setupMutableTree()
	for i := 0; i < 100; i++ {
		wg := new(sync.WaitGroup)
		it := NewIterator(nil, nil, true, tree.ImmutableTree)
		for j := 0; j < maxIterator; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				_, err := tree.Set([]byte(fmt.Sprintf("%d%d", i, j)), iavlrand.RandBytes(1))
				require.NoError(t, err)
			}(i, j)
		}
		for ; it.Valid(); it.Next() { //nolint:revive
		} // do nothing
		wg.Wait()
	}
}

func TestDelete(t *testing.T) {
	tree := setupMutableTree()

	_, err := tree.set([]byte("k1"), []byte("Fred"))
	require.NoError(t, err)
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	require.NoError(t, tree.DeleteVersionsTo(version))

	proof, err := tree.GetVersionedProof([]byte("k1"), version)
	require.EqualError(t, err, ErrVersionDoesNotExist.Error())
	require.Nil(t, proof)

	proof, err = tree.GetVersionedProof([]byte("k1"), version+1)
	require.Nil(t, err)
	require.Equal(t, 0, bytes.Compare([]byte("Fred"), proof.GetExist().Value))
}

func TestGetRemove(t *testing.T) {
	require := require.New(t)
	tree := setupMutableTree()
	testGet := func(exists bool) {
		v, err := tree.Get(tKey1)
		require.NoError(err)
		if exists {
			require.Equal(tVal1, v, "key should exist")
		} else {
			require.Nil(v, "key should not exist")
		}
	}

	testGet(false)

	ok, err := tree.Set(tKey1, tVal1)
	require.NoError(err)
	require.False(ok, "new key set: nothing to update")

	// add second key to avoid tree.root removal
	ok, err = tree.Set(tKey2, tVal2)
	require.NoError(err)
	require.False(ok, "new key set: nothing to update")

	testGet(true)

	// Save to tree.ImmutableTree
	_, version, err := tree.SaveVersion()
	require.NoError(err)
	require.Equal(int64(1), version)

	testGet(true)

	v, ok, err := tree.Remove(tKey1)
	require.NoError(err)
	require.True(ok, "key should be removed")
	require.Equal(tVal1, v, "key should exist")

	testGet(false)
}

func TestTraverse(t *testing.T) {
	tree := setupMutableTree()

	for i := 0; i < 6; i++ {
		_, err := tree.set([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	require.Equal(t, 11, tree.nodeSize(), "Size of tree unexpected")
}

func TestMutableTree_DeleteVersionsTo(t *testing.T) {
	tree := setupMutableTree()

	type entry struct {
		key   []byte
		value []byte
	}

	versionEntries := make(map[int64][]entry)

	// create 10 tree versions, each with 1000 random key/value entries
	for i := 0; i < 10; i++ {
		entries := make([]entry, 1000)

		for j := 0; j < 1000; j++ {
			k := iavlrand.RandBytes(10)
			v := iavlrand.RandBytes(10)

			entries[j] = entry{k, v}
			_, err := tree.Set(k, v)
			require.NoError(t, err)
		}

		_, v, err := tree.SaveVersion()
		require.NoError(t, err)

		versionEntries[v] = entries
	}

	// delete even versions
	versionToDelete := int64(8)
	require.NoError(t, tree.DeleteVersionsTo(versionToDelete))

	// ensure even versions have been deleted
	for v := int64(1); v <= versionToDelete; v++ {
		_, err := tree.LoadVersion(v)
		require.Error(t, err)
	}

	// ensure odd number versions exist and we can query for all set entries
	for _, v := range []int64{9, 10} {
		_, err := tree.LoadVersion(v)
		require.NoError(t, err)

		for _, e := range versionEntries[v] {
			val, err := tree.Get(e.key)
			require.NoError(t, err)
			if !bytes.Equal(e.value, val) {
				t.Log(val)
			}
			// require.Equal(t, e.value, val)
		}
	}
}

func TestMutableTree_LoadVersion_Empty(t *testing.T) {
	tree := setupMutableTree()

	version, err := tree.LoadVersion(0)
	require.NoError(t, err)
	assert.EqualValues(t, 0, version)

	version, err = tree.LoadVersion(-1)
	require.NoError(t, err)
	assert.EqualValues(t, 0, version)

	_, err = tree.LoadVersion(3)
	require.Error(t, err)
}

func TestMutableTree_InitialVersion(t *testing.T) {
	memDB := dbm.NewMemDB()
	tree := NewMutableTree(memDB, 0, log.NewNopLogger(), InitialVersionOption(9))

	_, err := tree.Set([]byte("a"), []byte{0x01})
	require.NoError(t, err)
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 9, version)

	_, err = tree.Set([]byte("b"), []byte{0x02})
	require.NoError(t, err)
	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 10, version)

	// Reloading the tree with the same initial version is fine
	tree = NewMutableTree(memDB, 0, log.NewNopLogger(), InitialVersionOption(9))
	version, err = tree.Load()
	require.NoError(t, err)
	assert.EqualValues(t, 10, version)

	// Reloading the tree with an initial version beyond the lowest should error
	tree = NewMutableTree(memDB, 0, log.NewNopLogger(), InitialVersionOption(10))
	_, err = tree.Load()
	require.Error(t, err)

	// Reloading the tree with a lower initial version is fine, and new versions can be produced
	tree = NewMutableTree(memDB, 0, log.NewNopLogger(), InitialVersionOption(3))
	version, err = tree.Load()
	require.NoError(t, err)
	assert.EqualValues(t, 10, version)

	_, err = tree.Set([]byte("c"), []byte{0x03})
	require.NoError(t, err)
	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 11, version)
}

func TestMutableTree_SetInitialVersion(t *testing.T) {
	tree := setupMutableTree()
	tree.SetInitialVersion(9)

	_, err := tree.Set([]byte("a"), []byte{0x01})
	require.NoError(t, err)
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	assert.EqualValues(t, 9, version)
}

func BenchmarkMutableTree_Set(b *testing.B) {
	db := dbm.NewMemDB()
	t := NewMutableTree(db, 100000, log.NewNopLogger())
	for i := 0; i < 1000000; i++ {
		_, err := t.Set(iavlrand.RandBytes(10), []byte{})
		require.NoError(b, err)
	}
	b.ReportAllocs()
	runtime.GC()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := t.Set(iavlrand.RandBytes(10), []byte{})
		require.NoError(b, err)
	}
}

func prepareTree(t *testing.T) *MutableTree {
	mdb := dbm.NewMemDB()
	tree := NewMutableTree(mdb, 1000, log.NewNopLogger())
	for i := 0; i < 100; i++ {
		_, err := tree.Set([]byte{byte(i)}, []byte("a"))
		require.NoError(t, err)
	}
	_, ver, err := tree.SaveVersion()
	require.True(t, ver == 1)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		_, err = tree.Set([]byte{byte(i)}, []byte("b"))
		require.NoError(t, err)
	}
	_, ver, err = tree.SaveVersion()
	require.True(t, ver == 2)
	require.NoError(t, err)

	newTree := NewMutableTree(mdb, 1000, log.NewNopLogger())

	return newTree
}

func TestMutableTree_Version(t *testing.T) {
	tree := prepareTree(t)
	require.True(t, tree.VersionExists(1))
	require.True(t, tree.VersionExists(2))
	require.False(t, tree.VersionExists(3))

	v, err := tree.GetLatestVersion()
	require.NoError(t, err)
	require.Equal(t, int64(2), v)
}

func checkGetVersioned(t *testing.T, tree *MutableTree, version int64, key, value []byte) {
	val, err := tree.GetVersioned(key, version)
	require.NoError(t, err)
	require.True(t, bytes.Equal(val, value))
}

func TestMutableTree_GetVersioned(t *testing.T) {
	tree := prepareTree(t)
	ver, err := tree.LoadVersion(1)
	require.True(t, ver == 2)
	require.NoError(t, err)
	// check key of unloaded version
	checkGetVersioned(t, tree, 1, []byte{1}, []byte("a"))
	checkGetVersioned(t, tree, 2, []byte{1}, []byte("b"))
	checkGetVersioned(t, tree, 3, []byte{1}, nil)

	tree = prepareTree(t)
	ver, err = tree.LoadVersion(2)
	require.True(t, ver == 2)
	require.NoError(t, err)
	checkGetVersioned(t, tree, 1, []byte{1}, []byte("a"))
	checkGetVersioned(t, tree, 2, []byte{1}, []byte("b"))
	checkGetVersioned(t, tree, 3, []byte{1}, nil)
}

func TestMutableTree_DeleteVersion(t *testing.T) {
	tree := prepareTree(t)
	ver, err := tree.LoadVersion(2)
	require.True(t, ver == 2)
	require.NoError(t, err)

	require.NoError(t, tree.DeleteVersionsTo(1))

	require.False(t, tree.VersionExists(1))
	require.True(t, tree.VersionExists(2))
	require.False(t, tree.VersionExists(3))

	// cannot delete latest version
	require.Error(t, tree.DeleteVersionsTo(2))
}

func TestMutableTree_LazyLoadVersionWithEmptyTree(t *testing.T) {
	mdb := dbm.NewMemDB()
	tree := NewMutableTree(mdb, 1000, log.NewNopLogger())
	_, v1, err := tree.SaveVersion()
	require.NoError(t, err)

	newTree1 := NewMutableTree(mdb, 1000, log.NewNopLogger())
	v2, err := newTree1.LoadVersion(1)
	require.NoError(t, err)
	require.True(t, v1 == v2)

	newTree2 := NewMutableTree(mdb, 1000, log.NewNopLogger())
	v2, err = newTree1.LoadVersion(1)
	require.NoError(t, err)
	require.True(t, v1 == v2)

	require.True(t, newTree1.root == newTree2.root)
}

func TestMutableTree_SetSimple(t *testing.T) {
	mdb := dbm.NewMemDB()
	tree := NewMutableTree(mdb, 0, log.NewNopLogger())

	const testKey1 = "a"
	const testVal1 = "test"

	isUpdated, err := tree.Set([]byte(testKey1), []byte(testVal1))
	require.NoError(t, err)
	require.False(t, isUpdated)

	fastValue, err := tree.Get([]byte(testKey1))
	require.NoError(t, err)
	_, regularValue, err := tree.GetWithIndex([]byte(testKey1))
	require.NoError(t, err)

	require.Equal(t, []byte(testVal1), fastValue)
	require.Equal(t, []byte(testVal1), regularValue)
}

func TestMutableTree_SetTwoKeys(t *testing.T) {
	tree := setupMutableTree()

	const testKey1 = "a"
	const testVal1 = "test"

	const testKey2 = "b"
	const testVal2 = "test2"

	isUpdated, err := tree.Set([]byte(testKey1), []byte(testVal1))
	require.NoError(t, err)
	require.False(t, isUpdated)

	isUpdated, err = tree.Set([]byte(testKey2), []byte(testVal2))
	require.NoError(t, err)
	require.False(t, isUpdated)

	fastValue, err := tree.Get([]byte(testKey1))
	require.NoError(t, err)
	_, regularValue, err := tree.GetWithIndex([]byte(testKey1))
	require.NoError(t, err)
	require.Equal(t, []byte(testVal1), fastValue)
	require.Equal(t, []byte(testVal1), regularValue)

	fastValue2, err := tree.Get([]byte(testKey2))
	require.NoError(t, err)
	_, regularValue2, err := tree.GetWithIndex([]byte(testKey2))
	require.NoError(t, err)
	require.Equal(t, []byte(testVal2), fastValue2)
	require.Equal(t, []byte(testVal2), regularValue2)
}

func TestMutableTree_SetOverwrite(t *testing.T) {
	tree := setupMutableTree()
	const testKey1 = "a"
	const testVal1 = "test"
	const testVal2 = "test2"

	isUpdated, err := tree.Set([]byte(testKey1), []byte(testVal1))
	require.NoError(t, err)
	require.False(t, isUpdated)

	isUpdated, err = tree.Set([]byte(testKey1), []byte(testVal2))
	require.NoError(t, err)
	require.True(t, isUpdated)

	fastValue, err := tree.Get([]byte(testKey1))
	require.NoError(t, err)
	_, regularValue, err := tree.GetWithIndex([]byte(testKey1))
	require.NoError(t, err)
	require.Equal(t, []byte(testVal2), fastValue)
	require.Equal(t, []byte(testVal2), regularValue)

}

func TestMutableTree_SetRemoveSet(t *testing.T) {
	tree := setupMutableTree()
	const testKey1 = "a"
	const testVal1 = "test"

	// Set 1
	isUpdated, err := tree.Set([]byte(testKey1), []byte(testVal1))
	require.NoError(t, err)
	require.False(t, isUpdated)

	value, err := tree.Get([]byte(testKey1))
	require.NoError(t, err)
	_, regularValue, err := tree.GetWithIndex([]byte(testKey1))
	require.NoError(t, err)
	require.Equal(t, []byte(testVal1), value)
	require.Equal(t, []byte(testVal1), regularValue)

	// Remove
	removedVal, isRemoved, err := tree.Remove([]byte(testKey1))
	require.NoError(t, err)
	require.NotNil(t, removedVal)
	require.True(t, isRemoved)

	value, err = tree.Get([]byte(testKey1))
	require.NoError(t, err)
	_, regularValue, err = tree.GetWithIndex([]byte(testKey1))
	require.NoError(t, err)
	require.Nil(t, value)
	require.Nil(t, regularValue)

	// Set 2
	isUpdated, err = tree.Set([]byte(testKey1), []byte(testVal1))
	require.NoError(t, err)
	require.False(t, isUpdated)
}

func TestIterate_MutableTree_Unsaved(t *testing.T) {
	tree, mirror := getRandomizedTreeAndMirror(t)
	assertMutableMirrorIterate(t, tree, mirror)
}

func TestIterate_MutableTree_Saved(t *testing.T) {
	tree, mirror := getRandomizedTreeAndMirror(t)

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	assertMutableMirrorIterate(t, tree, mirror)
}

func TestIterate_MutableTree_Unsaved_NextVersion(t *testing.T) {
	tree, mirror := getRandomizedTreeAndMirror(t)

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	assertMutableMirrorIterate(t, tree, mirror)

	randomizeTreeAndMirror(t, tree, mirror)

	assertMutableMirrorIterate(t, tree, mirror)
}

func TestIterator_MutableTree_Invalid(t *testing.T) {
	tree := getTestTree(0)

	itr, err := tree.Iterator([]byte("a"), []byte("b"), true)
	require.NoError(t, err)
	require.NotNil(t, itr)
	require.False(t, itr.Valid())
}

// TestMutableTree_InitialVersion_FirstVersion demonstrate the un-intuitive behavior,
// when InitialVersion is set the nodes created in the first version are not assigned with expected version number.
func TestMutableTree_InitialVersion_FirstVersion(t *testing.T) {
	db := dbm.NewMemDB()

	initialVersion := int64(1000)
	tree := NewMutableTree(db, 0, log.NewNopLogger(), InitialVersionOption(uint64(initialVersion)))

	_, err := tree.Set([]byte("hello"), []byte("world"))
	require.NoError(t, err)

	_, version, err := tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, initialVersion, version)
	rootKey := GetRootKey(version)
	// the nodes created at the first version are not assigned with the `InitialVersion`
	node, err := tree.ndb.GetNode(rootKey)
	require.NoError(t, err)
	require.Equal(t, initialVersion, node.nodeKey.version)

	_, err = tree.Set([]byte("hello"), []byte("world1"))
	require.NoError(t, err)

	_, version, err = tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, initialVersion+1, version)
	rootKey = GetRootKey(version)
	// the following versions behaves normally
	node, err = tree.ndb.GetNode(rootKey)
	require.NoError(t, err)
	require.Equal(t, initialVersion+1, node.nodeKey.version)
}

func TestMutableTreeClose(t *testing.T) {
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 0, log.NewNopLogger())

	_, err := tree.Set([]byte("hello"), []byte("world"))
	require.NoError(t, err)

	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	require.NoError(t, tree.Close())
}
