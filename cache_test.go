package iavl

import (
	"testing"

	rqr "github.com/stretchr/testify/require"
)

func randomData(vt *VersionedTree, versions, keysPerVersion int) {
	for i := 1; i <= versions; i++ {
		for j := 0; j < keysPerVersion; j++ {
			k := randBytes(8)
			v := randBytes(8)
			vt.Set(k, v)
		}
		vt.SaveVersion(int64(i))
	}
}

func TestCacheWrapVersionTree(t *testing.T) {
	require := rqr.New(t)

	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(100, d)
	versions := 50
	keysPerVersion := 30
	randomData(tree, versions, keysPerVersion)

	// let's calculate the root first
	tip := tree.Tree()
	saved := tip.Hash()

	// set one data and show it updates proper like
	k, v := randBytes(8), randBytes(8)
	tip.Set(k, v)
	_, vr := tip.Get(k)
	require.Equal(v, vr)
	update := tip.Hash()
	require.NotEqual(saved, update)

	// try various options on a cache
	cache := tip.CacheWrap()
	k2, v2 := randBytes(8), randBytes(8)
	cache.Set(k2, v2)
	cache.Remove(k)

	// must only affect the cache
	_, vr = cache.Get(k2)
	require.Equal(v2, vr)
	_, vr = cache.Get(k)
	require.Nil(vr)
	chash := cache.Hash()
	require.NotEqual(update, chash)

	// note that tip is not affected
	_, vr = tip.Get(k)
	require.Equal(v, vr)
	_, vr = tip.Get(k2)
	require.Nil(vr)
	uncached := tip.Hash()
	require.Equal(update, uncached)

	// write cache
	err := cache.Write()
	require.NoError(err)

	// show tip affected
	_, vr = tip.Get(k2)
	require.Equal(v2, vr)
	_, vr = tip.Get(k)
	require.Nil(vr)
	after := tip.Hash()
	require.Equal(chash, after)

	// save to disk, reload must show cached data
	working := int64(versions + 1)
	tree.SaveVersion(working)
	newTree := NewVersionedTree(100, d)
	err = newTree.LoadVersion(working)
	require.NoError(err)
	loaded := newTree.Tree()

	_, vr = loaded.Get(k2)
	require.Equal(v2, vr)
	_, vr = loaded.Get(k)
	require.Nil(vr)
	reload := loaded.Hash()
	require.Equal(chash, reload)
}

// TestCacheWrapVersionTree is same as above, but with Cache
// to demonstrate it works the same, but reuses the Tree struct
func TestCacheVersionTree(t *testing.T) {
	require := rqr.New(t)

	d, closeDB := getTestDB()
	defer closeDB()

	tree := NewVersionedTree(100, d)
	versions := 50
	keysPerVersion := 30
	randomData(tree, versions, keysPerVersion)

	// let's calculate the root first
	tip := tree.Tree()
	saved := tip.Hash()

	// set one data and show it updates proper like
	k, v := randBytes(8), randBytes(8)
	tip.Set(k, v)
	_, vr := tip.Get(k)
	require.Equal(v, vr)
	update := tip.Hash()
	require.NotEqual(saved, update)

	// be clear this must be a *Tree
	var cache *Tree
	cache = tip.Cache()
	k2, v2 := randBytes(8), randBytes(8)
	cache.Set(k2, v2)
	cache.Remove(k)

	// must only affect the cache
	_, vr = cache.Get(k2)
	require.Equal(v2, vr)
	_, vr = cache.Get(k)
	require.Nil(vr)
	chash := cache.Hash()
	require.NotEqual(update, chash)

	// note that tip is not affected
	_, vr = tip.Get(k)
	require.Equal(v, vr)
	_, vr = tip.Get(k2)
	require.Nil(vr)
	uncached := tip.Hash()
	require.Equal(update, uncached)

	// write cache by committing batch
	// (TODO: this must be public)
	cache.ndb.Commit()

	// show tip affected
	_, vr = tip.Get(k2)
	require.Equal(v2, vr)
	_, vr = tip.Get(k)
	require.Nil(vr)
	after := tip.Hash()
	require.Equal(chash, after)

	// save to disk, reload must show cached data
	working := int64(versions + 1)
	tree.SaveVersion(working)
	newTree := NewVersionedTree(100, d)
	err := newTree.LoadVersion(working)
	require.NoError(err)
	loaded := newTree.Tree()

	_, vr = loaded.Get(k2)
	require.Equal(v2, vr)
	_, vr = loaded.Get(k)
	require.Nil(vr)
	reload := loaded.Hash()
	require.Equal(chash, reload)
}
