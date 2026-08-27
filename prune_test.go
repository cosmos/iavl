package iavl

import (
	"fmt"
	"testing"
	"time"

	dbm "github.com/cosmos/iavl/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsyncPruning(t *testing.T) {
	db, err := dbm.NewGoLevelDB("test", t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	tree := NewMutableTree(db, 0, false, NewNopLogger(), AsyncPruningOption(true), FlushThresholdOption(1000))

	toVersion := 10000
	keyCount := 10
	pruneInterval := int64(100)
	keepRecent := int64(300)
	for i := 0; i < toVersion; i++ {
		for j := 0; j < keyCount; j++ {
			_, err := tree.Set([]byte(fmt.Sprintf("key-%d-%d", i, j)), []byte(fmt.Sprintf("value-%d-%d", i, j)))
			require.NoError(t, err)
		}

		tree.SetCommitting()
		_, v, err := tree.SaveVersion()
		require.NoError(t, err)
		tree.UnsetCommitting()

		if v%pruneInterval == 0 && v > keepRecent {
			ti := time.Now()
			require.NoError(t, tree.DeleteVersionsTo(v-keepRecent))
			t.Logf("Pruning %d versions took %v\n", keepRecent, time.Since(ti))
		}
	}

	// wait for async pruning to finish
	for i := 0; i < 100; i++ {
		tree.SetCommitting()
		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
		tree.UnsetCommitting()

		firstVersion, err := tree.ndb.getFirstVersion()
		require.NoError(t, err)
		t.Logf("Iteration: %d First version: %d\n", i, firstVersion)
		if firstVersion == int64(toVersion)-keepRecent+1 {
			break
		}
		// simulate the consensus process
		time.Sleep(500 * time.Millisecond)
	}

	// Reload the tree
	tree = NewMutableTree(db, 0, false, NewNopLogger())
	_, err = tree.LoadVersion(int64(toVersion) - keepRecent)
	require.Error(t, err)
	versions := tree.AvailableVersions()
	require.Equal(t, versions[0], toVersion-int(keepRecent)+1)
	for _, v := range versions {
		_, err := tree.LoadVersion(int64(v))
		require.NoError(t, err)
	}
}

// threeVersions builds a tree where versions 2 and 3 rewrite only the leftmost
// path, so each root's left child is new and its right subtree is shared.
func threeVersions(t *testing.T, db *dbm.MemDB) (tree *MutableTree, v1, v2, v3 int64) {
	t.Helper()
	tree = NewMutableTree(db, 0, true, NewNopLogger())
	set := func(key, value []byte) {
		_, err := tree.Set(key, value)
		require.NoError(t, err)
	}
	save := func() int64 {
		_, version, err := tree.SaveVersion()
		require.NoError(t, err)
		return version
	}
	for i := 0; i < 200; i++ {
		set(fmt.Appendf(nil, "key%03d", i), []byte{byte(i)})
	}
	v1 = save()
	set([]byte("key000"), []byte("x"))
	v2 = save()
	set([]byte("key001"), []byte("y"))
	v3 = save()
	return tree, v1, v2, v3
}

// rootChildren returns the node keys of a version root's two children.
func rootChildren(t *testing.T, ndb *nodeDB, version int64) (left, right []byte) {
	t.Helper()
	rootKey, err := ndb.GetRoot(version)
	require.NoError(t, err)
	root, err := ndb.GetNode(rootKey)
	require.NoError(t, err)
	return ndb.nodeKey(root.leftNodeKey), ndb.nodeKey(root.rightNodeKey)
}

func reachableNodeKeys(t *testing.T, ndb *nodeDB, version int64) map[string]bool {
	t.Helper()
	root, err := ndb.GetRoot(version)
	require.NoError(t, err)
	it, err := NewNodeIterator(root, ndb)
	require.NoError(t, err)
	keys := map[string]bool{}
	for ; it.Valid(); it.Next(false) {
		keys[string(ndb.nodeKey(it.GetNode().GetKey()))] = true
	}
	require.NoError(t, it.Error())
	return keys
}

// TestPruningKeepsLiveNodes: an unreadable node must not cost a later version
// its live nodes. Pruning fails instead, naming the version so it is retried.
func TestPruningKeepsLiveNodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		// victim is the node to break; erase deletes it outright rather than
		// only failing to read it.
		victim string
		erase  bool
	}{
		{"shared node missing", "shared", true},
		{"new node missing", "rewritten", true},
		{"new node unreadable", "rewritten", false},
		{"orphan unreadable", "orphan", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := dbm.NewMemDB()
			tree, v1, v2, v3 := threeVersions(t, db)
			live := reachableNodeKeys(t, tree.ndb, v3)

			rewritten, shared := rootChildren(t, tree.ndb, v2)
			orphan, sharedV1 := rootChildren(t, tree.ndb, v1)
			require.Equal(t, shared, sharedV1, "versions 1 and 2 must share the right subtree")

			// Pruning version 1 walks both versions. Only version 1 holds the
			// orphan, so tripping over it is caught only after version 2's walk.
			var victim []byte
			var stuck int64
			switch tc.victim {
			case "shared":
				victim, stuck = shared, v2
			case "rewritten":
				victim, stuck = rewritten, v2
			case "orphan":
				victim, stuck = orphan, v1
			default:
				t.Fatalf("unknown victim %q", tc.victim)
			}

			pruneDB := dbm.DB(db)
			if tc.erase {
				require.NoError(t, db.Delete(victim))
			} else {
				has, err := db.Has(victim)
				require.NoError(t, err)
				require.True(t, has, "the node must stay on disk; only the read fails")
				pruneDB = &unreadableDB{DB: db, key: victim}
			}

			// A fresh tree, so nothing is answered from the node cache.
			fresh := NewMutableTree(pruneDB, 0, true, NewNopLogger())
			_, err := fresh.LoadVersion(v3)
			require.NoError(t, err)
			err = fresh.DeleteVersionsTo(v1)
			require.Error(t, err, "pruning read a broken node and must say so")
			assert.Contains(t, err.Error(), fmt.Sprint("traversing version ", stuck),
				"the error must name the version the pruner is stuck on")

			for nk := range live {
				if tc.erase && nk == string(victim) {
					continue // the one node the test itself removed
				}
				has, err := db.Has([]byte(nk))
				require.NoError(t, err)
				require.True(t, has, "pruning deleted a node version %d still references", v3)
			}
		})
	}
}
