package iavl

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"testing"

	"github.com/cosmos/iavl/mock"
	"github.com/golang/mock/gomock"
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
			val := tree.GetFast(e.key)
			require.Equal(t, e.value, val)
		}
	}
}

func TestMutableTree_LoadVersion_Empty(t *testing.T) {
	memDB := db.NewMemDB()
	tree, err := NewMutableTree(memDB, 0)
	require.NoError(t, err)

	version, err := tree.LoadVersion(0)
	require.NoError(t, err)
	assert.EqualValues(t, 0, version)

	version, err = tree.LoadVersion(-1)
	require.NoError(t, err)
	assert.EqualValues(t, 0, version)

	_, err = tree.LoadVersion(3)
	require.Error(t, err)
}

func TestMutableTree_LazyLoadVersion_Empty(t *testing.T) {
	memDB := db.NewMemDB()
	tree, err := NewMutableTree(memDB, 0)
	require.NoError(t, err)

	version, err := tree.LazyLoadVersion(0)
	require.NoError(t, err)
	assert.EqualValues(t, 0, version)

	version, err = tree.LazyLoadVersion(-1)
	require.NoError(t, err)
	assert.EqualValues(t, 0, version)

	_, err = tree.LazyLoadVersion(3)
	require.Error(t, err)
}

func TestMutableTree_DeleteVersionsRange(t *testing.T) {
	require := require.New(t)

	mdb := db.NewMemDB()
	tree, err := NewMutableTree(mdb, 0)
	require.NoError(err)

	const maxLength = 100
	const fromLength = 10

	versions := make([]int64, 0, maxLength)
	for count := 1; count <= maxLength; count++ {
		versions = append(versions, int64(count))
		countStr := strconv.Itoa(count)
		// Set kv pair and save version
		tree.Set([]byte("aaa"), []byte("bbb"))
		tree.Set([]byte("key"+countStr), []byte("value"+countStr))
		_, _, err = tree.SaveVersion()
		require.NoError(err, "SaveVersion should not fail")
	}

	tree, err = NewMutableTree(mdb, 0)
	require.NoError(err)
	targetVersion, err := tree.LoadVersion(int64(maxLength))
	require.NoError(err)
	require.Equal(targetVersion, int64(maxLength), "targetVersion shouldn't larger than the actual tree latest version")

	err = tree.DeleteVersionsRange(fromLength, int64(maxLength/2))
	require.NoError(err, "DeleteVersionsTo should not fail")

	for _, version := range versions[:fromLength-1] {
		require.True(tree.versions[version], "versions %d no more than 10 should exist", version)

		v, err := tree.LazyLoadVersion(version)
		require.NoError(err, version)
		require.Equal(v, version)

		value := tree.GetFast([]byte("aaa"))
		require.Equal(string(value), "bbb")

		for _, count := range versions[:version] {
			countStr := strconv.Itoa(int(count))
			 value := tree.GetFast([]byte("key" + countStr))
			require.Equal(string(value), "value"+countStr)
		}
	}

	for _, version := range versions[fromLength : int64(maxLength/2)-1] {
		require.False(tree.versions[version], "versions %d more 10 and no more than 50 should have been deleted", version)

		_, err := tree.LazyLoadVersion(version)
		require.Error(err)
	}

	for _, version := range versions[int64(maxLength/2)-1:] {
		require.True(tree.versions[version], "versions %d more than 50 should exist", version)

		v, err := tree.LazyLoadVersion(version)
		require.NoError(err)
		require.Equal(v, version)

		value := tree.GetFast([]byte("aaa"))
		require.Equal(string(value), "bbb")

		for _, count := range versions[:fromLength] {
			countStr := strconv.Itoa(int(count))
			value := tree.GetFast([]byte("key" + countStr))
			require.Equal(string(value), "value"+countStr)
		}
		for _, count := range versions[int64(maxLength/2)-1 : version] {
			countStr := strconv.Itoa(int(count))
			value := tree.GetFast([]byte("key" + countStr))
			require.Equal(string(value), "value"+countStr)
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

func prepareTree(t *testing.T) *MutableTree {
	mdb := db.NewMemDB()
	tree, err := NewMutableTree(mdb, 1000)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		tree.Set([]byte{byte(i)}, []byte("a"))
	}
	_, ver, err := tree.SaveVersion()
	require.True(t, ver == 1)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		tree.Set([]byte{byte(i)}, []byte("b"))
	}
	_, ver, err = tree.SaveVersion()
	require.True(t, ver == 2)
	require.NoError(t, err)
	newTree, err := NewMutableTree(mdb, 1000)
	require.NoError(t, err)

	return newTree
}

func TestMutableTree_VersionExists(t *testing.T) {
	tree := prepareTree(t)
	require.True(t, tree.VersionExists(1))
	require.True(t, tree.VersionExists(2))
	require.False(t, tree.VersionExists(3))
}

func checkGetVersioned(t *testing.T, tree *MutableTree, version int64, key, value []byte) {
	val := tree.GetVersioned(key, version)
	require.True(t, bytes.Equal(val, value))
}

func TestMutableTree_GetVersioned(t *testing.T) {
	tree := prepareTree(t)
	ver, err := tree.LazyLoadVersion(1)
	require.True(t, ver == 1)
	require.NoError(t, err)
	// check key of unloaded version
	checkGetVersioned(t, tree, 1, []byte{1}, []byte("a"))
	checkGetVersioned(t, tree, 2, []byte{1}, []byte("b"))
	checkGetVersioned(t, tree, 3, []byte{1}, nil)

	tree = prepareTree(t)
	ver, err = tree.LazyLoadVersion(2)
	require.True(t, ver == 2)
	require.NoError(t, err)
	checkGetVersioned(t, tree, 1, []byte{1}, []byte("a"))
	checkGetVersioned(t, tree, 2, []byte{1}, []byte("b"))
	checkGetVersioned(t, tree, 3, []byte{1}, nil)
}

func TestMutableTree_DeleteVersion(t *testing.T) {
	tree := prepareTree(t)
	ver, err := tree.LazyLoadVersion(2)
	require.True(t, ver == 2)
	require.NoError(t, err)

	require.NoError(t, tree.DeleteVersion(1))

	require.False(t, tree.VersionExists(1))
	require.True(t, tree.VersionExists(2))
	require.False(t, tree.VersionExists(3))

	// cannot delete latest version
	require.Error(t, tree.DeleteVersion(2))
}

func TestMutableTree_LazyLoadVersionWithEmptyTree(t *testing.T) {
	mdb := db.NewMemDB()
	tree, err := NewMutableTree(mdb, 1000)
	require.NoError(t, err)
	_, v1, err := tree.SaveVersion()
	require.NoError(t, err)

	newTree1, err := NewMutableTree(mdb, 1000)
	require.NoError(t, err)
	v2, err := newTree1.LazyLoadVersion(1)
	require.NoError(t, err)
	require.True(t, v1 == v2)

	newTree2, err := NewMutableTree(mdb, 1000)
	require.NoError(t, err)
	v2, err = newTree1.LoadVersion(1)
	require.NoError(t, err)
	require.True(t, v1 == v2)

	require.True(t, newTree1.root == newTree2.root)
}

func TestMutableTree_SetSimple(t *testing.T) {
	mdb := db.NewMemDB()
	tree, err := NewMutableTree(mdb, 0)
	require.NoError(t, err)

	const testKey1 = "a"
	const testVal1 = "test"

	isUpdated := tree.Set([]byte(testKey1), []byte(testVal1))
	require.False(t, isUpdated)

	fastValue := tree.GetFast([]byte(testKey1))
	_, regularValue := tree.Get([]byte(testKey1))

	require.Equal(t, []byte(testVal1), fastValue)
	require.Equal(t, []byte(testVal1), regularValue)

	
	fastNodeAdditions := tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, 1, len(fastNodeAdditions))
	
	fastNodeAddition := fastNodeAdditions[testKey1]
	require.Equal(t, []byte(testKey1), fastNodeAddition.key)
	require.Equal(t, []byte(testVal1), fastNodeAddition.value)
	require.Equal(t, int64(1), fastNodeAddition.versionLastUpdatedAt)
}

func TestMutableTree_SetTwoKeys(t *testing.T) {
	mdb := db.NewMemDB()
	tree, err := NewMutableTree(mdb, 0)
	require.NoError(t, err)

	const testKey1 = "a"
	const testVal1 = "test"

	const testKey2 = "b"
	const testVal2 = "test2"

	isUpdated := tree.Set([]byte(testKey1), []byte(testVal1))
	require.False(t, isUpdated)

	isUpdated = tree.Set([]byte(testKey2), []byte(testVal2))
	require.False(t, isUpdated)

	fastValue := tree.GetFast([]byte(testKey1))
	_, regularValue := tree.Get([]byte(testKey1))
	require.Equal(t, []byte(testVal1), fastValue)
	require.Equal(t, []byte(testVal1), regularValue)

	fastValue2 := tree.GetFast([]byte(testKey2))
	_, regularValue2 := tree.Get([]byte(testKey2))
	require.Equal(t, []byte(testVal2), fastValue2)
	require.Equal(t, []byte(testVal2), regularValue2)

	fastNodeAdditions := tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, 2, len(fastNodeAdditions))
	
	fastNodeAddition := fastNodeAdditions[testKey1]
	require.Equal(t, []byte(testKey1), fastNodeAddition.key)
	require.Equal(t, []byte(testVal1), fastNodeAddition.value)
	require.Equal(t, int64(1), fastNodeAddition.versionLastUpdatedAt)

	fastNodeAddition = fastNodeAdditions[testKey2]
	require.Equal(t, []byte(testKey2), fastNodeAddition.key)
	require.Equal(t, []byte(testVal2), fastNodeAddition.value)
	require.Equal(t, int64(1), fastNodeAddition.versionLastUpdatedAt)
}

func TestMutableTree_SetOverwrite(t *testing.T) {
	mdb := db.NewMemDB()
	tree, err := NewMutableTree(mdb, 0)
	require.NoError(t, err)

	const testKey1 = "a"
	const testVal1 = "test"
	const testVal2 = "test2"

	isUpdated := tree.Set([]byte(testKey1), []byte(testVal1))
	require.False(t, isUpdated)

	isUpdated = tree.Set([]byte(testKey1), []byte(testVal2))
	require.True(t, isUpdated)

	fastValue := tree.GetFast([]byte(testKey1))
	_, regularValue := tree.Get([]byte(testKey1))
	require.Equal(t, []byte(testVal2), fastValue)
	require.Equal(t, []byte(testVal2), regularValue)

	
	fastNodeAdditions := tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, 1, len(fastNodeAdditions))
	
	fastNodeAddition := fastNodeAdditions[testKey1]
	require.Equal(t, []byte(testKey1), fastNodeAddition.key)
	require.Equal(t, []byte(testVal2), fastNodeAddition.value)
	require.Equal(t, int64(1), fastNodeAddition.versionLastUpdatedAt)
}

func TestMutableTree_SetRemoveSet(t *testing.T) {
	mdb := db.NewMemDB()
	tree, err := NewMutableTree(mdb, 0)
	require.NoError(t, err)

	const testKey1 = "a"
	const testVal1 = "test"

	// Set 1
	isUpdated := tree.Set([]byte(testKey1), []byte(testVal1))
	require.False(t, isUpdated)

	fastValue := tree.GetFast([]byte(testKey1))
	_, regularValue := tree.Get([]byte(testKey1))
	require.Equal(t, []byte(testVal1), fastValue)
	require.Equal(t, []byte(testVal1), regularValue)

	
	fastNodeAdditions := tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, 1, len(fastNodeAdditions))
	
	fastNodeAddition := fastNodeAdditions[testKey1]
	require.Equal(t, []byte(testKey1), fastNodeAddition.key)
	require.Equal(t, []byte(testVal1), fastNodeAddition.value)
	require.Equal(t, int64(1), fastNodeAddition.versionLastUpdatedAt)

	// Remove
	removedVal, isRemoved := tree.Remove([]byte(testKey1))
	require.NotNil(t, removedVal)
	require.True(t, isRemoved)

	fastNodeAdditions = tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, 0, len(fastNodeAdditions))

	fastNodeRemovals := tree.GetUnsavedFastNodeRemovals()
	require.Equal(t, 1, len(fastNodeRemovals))

	fastValue = tree.GetFast([]byte(testKey1))
	_, regularValue = tree.Get([]byte(testKey1))
	require.Nil(t, fastValue)
	require.Nil(t, regularValue)

	// Set 2
	isUpdated = tree.Set([]byte(testKey1), []byte(testVal1))
	require.False(t, isUpdated)

	fastValue = tree.GetFast([]byte(testKey1))
	_, regularValue = tree.Get([]byte(testKey1))
	require.Equal(t, []byte(testVal1), fastValue)
	require.Equal(t, []byte(testVal1), regularValue)

	
	fastNodeAdditions = tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, 1, len(fastNodeAdditions))
	
	fastNodeAddition = fastNodeAdditions[testKey1]
	require.Equal(t, []byte(testKey1), fastNodeAddition.key)
	require.Equal(t, []byte(testVal1), fastNodeAddition.value)
	require.Equal(t, int64(1), fastNodeAddition.versionLastUpdatedAt)

	fastNodeRemovals = tree.GetUnsavedFastNodeRemovals()
	require.Equal(t, 0, len(fastNodeRemovals))
}

func TestMutableTree_FastNodeIntegration(t *testing.T) {
	mdb := db.NewMemDB()
	tree, err := NewMutableTree(mdb, 1000)
	require.NoError(t, err)

	const key1 = "a"
	const key2 = "b"
	const key3 = "c"

	const testVal1 = "test"
	const testVal2 = "test2"

	// Set key1
	res := tree.Set([]byte(key1), []byte(testVal1))
	require.False(t, res)

	unsavedNodeAdditions := tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, len(unsavedNodeAdditions), 1)

	// Set key2
	res = tree.Set([]byte(key2), []byte(testVal1))
	require.False(t, res)

	unsavedNodeAdditions = tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, len(unsavedNodeAdditions), 2)

	// Set key3
	res = tree.Set([]byte(key3), []byte(testVal1))
	require.False(t, res)

	unsavedNodeAdditions = tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, len(unsavedNodeAdditions), 3)

	// Set key3 with new value
	res = tree.Set([]byte(key3), []byte(testVal2))
	require.True(t, res)

	unsavedNodeAdditions = tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, len(unsavedNodeAdditions), 3)

	// Remove key2
	removedVal, isRemoved := tree.Remove([]byte(key2))
	require.True(t, isRemoved)
	require.Equal(t, []byte(testVal1), removedVal)

	unsavedNodeAdditions = tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, len(unsavedNodeAdditions), 2)

	unsavedNodeRemovals := tree.GetUnsavedFastNodeRemovals()
	require.Equal(t, len(unsavedNodeRemovals), 1)

	// Save
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	unsavedNodeAdditions = tree.GetUnsavedFastNodeAdditions()
	require.Equal(t, len(unsavedNodeAdditions), 0)

	unsavedNodeRemovals = tree.GetUnsavedFastNodeRemovals()
	require.Equal(t, len(unsavedNodeRemovals), 0)

	// Load
	t2, err := NewMutableTree(mdb, 0)
	require.NoError(t, err)
	
	_, err = t2.Load()
	require.NoError(t, err)

	// Get and GetFast
	fastValue := t2.GetFast([]byte(key1))
	_, regularValue := tree.Get([]byte(key1))
	require.Equal(t, []byte(testVal1), fastValue)
	require.Equal(t, []byte(testVal1), regularValue)

	fastValue = t2.GetFast([]byte(key2))
	_, regularValue = t2.Get([]byte(key2))
	require.Nil(t, fastValue)
	require.Nil(t, regularValue)

	fastValue = t2.GetFast([]byte(key3))
	_, regularValue = tree.Get([]byte(key3))
	require.Equal(t, []byte(testVal2), fastValue)
	require.Equal(t, []byte(testVal2), regularValue)
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
	tree, err := getTestTree(0)
	require.NoError(t, err)

	itr := tree.Iterator([]byte("a"), []byte("b"), true)

	require.NotNil(t, itr)
	require.False(t, itr.Valid())
}

func TestUpgradeStorageToFastCache_LatestVersion_Success(t *testing.T) {
	// Setup
	db := db.NewMemDB()
	oldTree := newMutableTreeWithOpts(db, 1000, nil)
	mirror := make(map[string]string)
	// Fill with some data
	randomizeTreeAndMirror(t, oldTree, mirror)

	require.True(t, oldTree.IsLatestTreeVersion())
	require.Equal(t, defaultStorageVersionValue, oldTree.GetStorageVersion())

	// Test new tree from not upgraded db, should upgrade
	sut, err := NewMutableTree(db, 0)
	require.NoError(t, err)
	require.Equal(t, fastStorageVersionValue, sut.GetStorageVersion())
}

func TestUpgrade_AlreadyUpgraded_Success(t *testing.T) {
	// Setup
	db := db.NewMemDB()
	oldTree := newMutableTreeWithOpts(db, 1000, nil)
	mirror := make(map[string]string)
	// Fill with some data
	randomizeTreeAndMirror(t, oldTree, mirror)
	// Upgrade
	require.NoError(t, oldTree.ndb.upgradeToFastCacheFromLeaves())
	require.Equal(t, fastStorageVersionValue, oldTree.GetStorageVersion())

	// Test new tree from upgraded db
	sut, err := NewMutableTree(db, 0)
	require.NoError(t, err)
	require.Equal(t, fastStorageVersionValue, sut.GetStorageVersion())
}

func TestUpgradeStorageToFastCache_DbError_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(defaultStorageVersionValue), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	expectedError := errors.New("some db error")

	dbMock.EXPECT().Iterator(gomock.Any(), gomock.Any()).Return(nil, expectedError).Times(1)

	tree, err := NewMutableTree(dbMock, 0)
	require.Equal(t, expectedError, err)
	require.Nil(t, tree)
}

func TestUpgradeStorageToFastCache_Integration_Upgraded_FastIterator_Success(t *testing.T) {	
	oldTree, mirror := setupTreeAndMirrorForUpgrade(t)
	require.Equal(t, defaultStorageVersionValue, oldTree.GetStorageVersion())

	sut, err := NewMutableTreeWithOpts(oldTree.ndb.db, 100, nil)
	require.NoError(t, err)
	require.NotNil(t, sut)
	require.Equal(t, fastStorageVersionValue, sut.GetStorageVersion())

	// Load version
	version, err := sut.Load()
	require.NoError(t, err)
	require.Equal(t, int64(1), version)
	
	// Test that upgraded mutable tree iterates as expected
	t.Run("Mutable tree", func (t *testing.T)  {
		i := 0
		oldTree.Iterate(func (k, v []byte) bool {
			require.Equal(t, []byte(mirror[i][0]), k)
			require.Equal(t, []byte(mirror[i][1]), v)
			i++
			return false	
		})
	})

	// Test that upgraded immutable tree iterates as expected
	t.Run("Immutable tree", func (t *testing.T)  {
		immutableTree, err := oldTree.GetImmutable(oldTree.version)
		require.NoError(t, err)

		i := 0
		immutableTree.Iterate(func (k, v []byte) bool {
			require.Equal(t, []byte(mirror[i][0]), k)
			require.Equal(t, []byte(mirror[i][1]), v)
			i++
			return false	
		})
	})
}

func TestUpgradeStorageToFastCache_Integration_Upgraded_GetFast_Success(t *testing.T) {
	oldTree, mirror := setupTreeAndMirrorForUpgrade(t)
	require.Equal(t, defaultStorageVersionValue, oldTree.GetStorageVersion())

	sut, err := NewMutableTreeWithOpts(oldTree.ndb.db, 100, nil)
	require.NoError(t, err)
	require.NotNil(t, sut)
	require.Equal(t, fastStorageVersionValue, sut.GetStorageVersion())

	// Lazy Load version
	version, err := sut.LazyLoadVersion(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), version)

	t.Run("Mutable tree", func (t *testing.T)  {
		for _, kv := range mirror {
			v := sut.GetFast([]byte(kv[0]))
			require.Equal(t, []byte(kv[1]), v)
		}
	})

	t.Run("Immutable tree", func (t *testing.T)  {
		immutableTree, err := sut.GetImmutable(sut.version)
		require.NoError(t, err)

		for _, kv := range mirror {
			v := immutableTree.GetFast([]byte(kv[0]))
			require.Equal(t, []byte(kv[1]), v)
		}
	})
}

func setupTreeAndMirrorForUpgrade(t *testing.T) (*MutableTree, [][]string) {
	db := db.NewMemDB()

	tree := newMutableTreeWithOpts(db, 0, nil)

	var keyPrefix, valPrefix string = "key", "val"

	mirror := make([][]string, 0, 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s_%d", keyPrefix, i)
		val := fmt.Sprintf("%s_%d", valPrefix, i)
		mirror = append(mirror, []string{key, val})
		require.False(t, tree.Set([]byte(key), []byte(val)))
	}

	_, _, err := tree.SaveVersion()
	require.NoError(t, err)

	// Delete fast nodes from database to mimic a version with no upgrade
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s_%d", keyPrefix, i)
		require.NoError(t, db.Delete(fastKeyFormat.Key([]byte(key))))
	}
	return tree, mirror
}
