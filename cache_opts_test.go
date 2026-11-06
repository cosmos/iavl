package iavl

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
)

func TestCacheValueSizeLimit(t *testing.T) {
	small, big := []byte("small"), bytes.Repeat([]byte("x"), 64)
	tree := NewMutableTree(dbm.NewMemDB(), 1000, false, NewNopLogger(), CacheValueSizeLimitOption(16))
	_, err := tree.Set([]byte("a"), small)
	require.NoError(t, err)
	_, err = tree.Set([]byte("b"), big)
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	ndb := tree.ndb
	require.True(t, ndb.fastNodeCache.Has([]byte("a")))
	require.False(t, ndb.fastNodeCache.Has([]byte("b")), "large fast node must not be cached")
	var bigLeaf int
	leaves, err := ndb.leafNodes()
	require.NoError(t, err)
	for _, l := range leaves {
		cached := ndb.nodeCache.Has(l.GetKey())
		if len(l.value) > 16 {
			bigLeaf++
			require.False(t, cached, "large leaf must not be cached")
		} else {
			require.True(t, cached)
		}
	}
	require.Equal(t, 1, bigLeaf)

	// reads of the large value still work and don't populate the caches.
	v, err := tree.Get([]byte("b"))
	require.NoError(t, err)
	require.Equal(t, big, v)
	require.False(t, ndb.fastNodeCache.Has([]byte("b")))

	// growing a cached value past the limit must drop the stale cache entry.
	_, err = tree.Set([]byte("a"), big)
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	require.False(t, ndb.fastNodeCache.Has([]byte("a")))
	v, err = tree.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, big, v)
}

func TestFastNodeCacheSizeOption(t *testing.T) {
	db := dbm.NewMemDB()
	require.Equal(t, defaultFastNodeCacheSize, fastCacheCap(NewMutableTree(db, 0, false, NewNopLogger())))

	tree := NewMutableTree(db, 0, false, NewNopLogger(), FastNodeCacheSizeOption(3))
	for i := 0; i < 10; i++ {
		_, err := tree.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
		require.NoError(t, err)
	}
	_, _, err := tree.SaveVersion()
	require.NoError(t, err)
	require.Equal(t, 3, tree.ndb.fastNodeCache.Len())
}

func fastCacheCap(tree *MutableTree) int {
	for i := 0; ; i++ {
		tree.ndb.fastNodeCache.Add(&testCacheNode{key: []byte(fmt.Sprintf("%d", i))})
		if tree.ndb.fastNodeCache.Len() <= i {
			return i
		}
		if i > 2*defaultFastNodeCacheSize {
			return -1
		}
	}
}

type testCacheNode struct{ key []byte }

func (n *testCacheNode) GetKey() []byte { return n.key }

func TestPruningEvictsAndSkipsCache(t *testing.T) {
	db := dbm.NewMemDB()
	tree := NewMutableTree(db, 100000, true, NewNopLogger())
	for v := 0; v < 5; v++ {
		for i := 0; i < 10; i++ {
			_, err := tree.Set([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d-%d", v, i)))
			require.NoError(t, err)
		}
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
	}
	ndb := tree.ndb

	// nodes orphaned by the version update.
	var orphans [][]byte
	require.NoError(t, ndb.traverseOrphans(1, 2, func(o *Node) error {
		orphans = append(orphans, o.GetKey())
		return nil
	}))
	require.NotEmpty(t, orphans)
	for _, k := range orphans {
		require.True(t, ndb.nodeCache.Has(k), "setup: orphan should be cached")
	}

	require.NoError(t, ndb.DeleteVersionsTo(1))
	require.NoError(t, ndb.Commit())
	// version 1 pruned: its orphans are gone from disk and must be gone from the cache.
	require.NoError(t, ndb.DeleteVersionsTo(2))
	require.NoError(t, ndb.Commit())
	for _, k := range orphans {
		require.False(t, ndb.nodeCache.Has(k), "pruned node still cached: %x", k)
		has, err := ndb.Has(k)
		require.NoError(t, err)
		require.False(t, has)
	}

	// a pruning read of an uncached node must not populate the cache.
	latest, err := ndb.GetRoot(5)
	require.NoError(t, err)
	ndb.nodeCache.Remove(latest)
	_, err = ndb.getNode(latest, true)
	require.NoError(t, err)
	require.False(t, ndb.nodeCache.Has(latest))
}
