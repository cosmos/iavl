package iavl

import (
	"bytes"
	"fmt"
	"maps"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	corestore "cosmossdk.io/core/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
	"github.com/cosmos/iavl/fastnode"
)

func TestReadPreviousVersionDuringSaveVersion(t *testing.T) {
	const (
		timeout    = 10 * time.Second
		updatedKey = "key000"
		removedKey = "key001"
		addedKey   = "new"
	)

	// More keys than a MemDB iterator buffers, so an open fast iterator holds MemDB's lock until it is closed.
	previousState := make(map[string]string, 100)
	for i := range 100 {
		previousState[fmt.Sprintf("key%03d", i)] = "1"
	}
	keys := make([]string, 0, len(previousState)+1)
	keys = append(keys, addedKey)
	for key := range previousState {
		keys = append(keys, key)
	}

	read := func(tree *ImmutableTree) treeReads {
		got := treeReads{fetched: map[string]string{}}
		if got.iterated, got.err = iterateAll(tree); got.err != nil {
			return got
		}
		for _, key := range keys {
			value, err := tree.Get([]byte(key))
			if err != nil {
				got.err = err
				return got
			}
			if value != nil {
				got.fetched[key] = string(value)
			}
		}
		return got
	}

	// readOnceFastNodesAreInDB reads the previous version after the next version's fast nodes reached the DB,
	// through the batch's flush threshold or writeBatch, and before the next version is saved.
	readOnceFastNodesAreInDB := func(t *testing.T, db *iteratorHookDB, tree *MutableTree, readPrevious func() treeReads, writeBatch func() error) treeReads {
		additions, removals := tree.getUnsavedFastNodeAdditions(), tree.getUnsavedFastNodeRemovals()
		// SaveVersion starts here.
		require.NoError(t, tree.saveFastNodeVersion(tree.WorkingVersion()))
		if writeBatch != nil {
			require.NoError(t, writeBatch())
		}
		for key := range additions {
			value, err := db.Get(fastKeyFormat.KeyBytes([]byte(key)))
			require.NoError(t, err)
			node, err := fastnode.DeserializeNode([]byte(key), value)
			require.NoError(t, err)
			require.Equal(t, tree.WorkingVersion(), node.GetVersionLastUpdatedAt(), "fast node of %q did not reach the DB before the save", key)
		}
		for key := range removals {
			value, err := db.Get(fastKeyFormat.KeyBytes([]byte(key)))
			require.NoError(t, err)
			require.Nil(t, value, "fast node removal of %q did not reach the DB before the save", key)
		}

		got := readPrevious()
		versioned := map[string]string{}
		for _, key := range keys {
			value, err := tree.GetVersioned([]byte(key), tree.Version())
			require.NoError(t, err)
			if value != nil {
				versioned[key] = string(value)
			}
		}
		assert.Equal(t, previousState, versioned, "GetVersioned of the previous version")

		_, _, err := tree.SaveVersion()
		require.NoError(t, err)
		return got
	}

	// A threshold of one byte makes the batch flush every fast node change at once.
	flushEarly := []Option{FlushThresholdOption(1)}
	readAfterEarlyFlush := func(t *testing.T, db *iteratorHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads {
		return readOnceFastNodesAreInDB(t, db, tree, readPrevious, nil)
	}

	testCases := []struct {
		name   string
		opts   []Option
		set    map[string]string // keys the next version adds or updates
		remove []string          // keys the next version removes
		// readDuringSave saves the next version and returns what a reader of the previous version saw on the way.
		readDuringSave func(t *testing.T, db *iteratorHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads
	}{
		{
			name:           "added, updated and removed fast nodes are flushed before the commit",
			opts:           flushEarly,
			set:            map[string]string{updatedKey: "2", addedKey: "2"},
			remove:         []string{removedKey},
			readDuringSave: readAfterEarlyFlush,
		},
		{
			name:           "only added and updated fast nodes are flushed before the commit",
			opts:           flushEarly,
			set:            map[string]string{updatedKey: "2", addedKey: "2"},
			readDuringSave: readAfterEarlyFlush,
		},
		{
			name:           "only a removed fast node is flushed before the commit",
			opts:           flushEarly,
			remove:         []string{removedKey},
			readDuringSave: readAfterEarlyFlush,
		},
		{
			name:   "pruning commits the batch while the next version is being saved",
			set:    map[string]string{updatedKey: "2", addedKey: "2"},
			remove: []string{removedKey},
			readDuringSave: func(t *testing.T, db *iteratorHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads {
				// DeleteVersionsTo may run beside SaveVersion, and commits the batch the two share.
				prune := func() error { return tree.DeleteVersionsTo(0) }
				return readOnceFastNodesAreInDB(t, db, tree, readPrevious, prune)
			},
		},
		{
			name:   "next version commits between the version check and the fast iterator snapshot",
			set:    map[string]string{updatedKey: "2", addedKey: "2"},
			remove: []string{removedKey},
			readDuringSave: func(t *testing.T, db *iteratorHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads {
				save := func() {
					_, _, err := tree.SaveVersion()
					require.NoError(t, err)
				}
				db.beforeFastIterator.Store(&save)

				got := readPrevious()
				require.Nil(t, db.beforeFastIterator.Load(), "the read opened no fast iterator")
				return got
			},
		},
		{
			name:   "latest version is loaded from the DB while the next version is being saved",
			opts:   flushEarly,
			set:    map[string]string{updatedKey: "2", addedKey: "2"},
			remove: []string{removedKey},
			readDuringSave: func(t *testing.T, db *iteratorHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads {
				return readAfterEarlyFlush(t, db, tree, func() treeReads {
					// A zero latest version is loaded from the DB by the next read. That load must not publish the dirty fast index.
					tree.ndb.mtx.Lock()
					tree.ndb.latestVersion = 0
					tree.ndb.mtx.Unlock()
					return readPrevious()
				})
			},
		},
		{
			name:   "next version starts committing while the fast iterator is open",
			set:    map[string]string{updatedKey: "2", addedKey: "2"},
			remove: []string{removedKey},
			readDuringSave: func(t *testing.T, db *iteratorHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads {
				saved := make(chan error, 1)
				// The open iterator blocks MemDB's batch write, so the commit holds nodeDB's lock until the reader closes it.
				startSave := func() {
					go func() {
						_, _, err := tree.SaveVersion()
						saved <- err
					}()
					assert.Eventually(t, func() bool {
						return goroutineBlockedIn("(*memDBBatch).Write")
					}, timeout, time.Millisecond, "SaveVersion never reached the batch write")
				}
				db.afterFastIterator.Store(&startSave)

				done := make(chan treeReads, 1)
				go func() { done <- readPrevious() }()

				var got treeReads
				select {
				case got = <-done:
				case <-time.After(timeout):
					require.FailNow(t, "the reader and SaveVersion deadlocked")
				}
				require.Nil(t, db.afterFastIterator.Load(), "the read opened no fast iterator")

				select {
				case err := <-saved:
					require.NoError(t, err)
				case <-time.After(timeout):
					require.FailNow(t, "SaveVersion did not finish after the read")
				}
				return got
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := &iteratorHookDB{KVStoreWithBatch: dbm.NewMemDB()}
			tree := NewMutableTree(db, 0, false, NewNopLogger(), tc.opts...)
			for key, value := range previousState {
				_, err := tree.Set([]byte(key), []byte(value))
				require.NoError(t, err)
			}
			_, _, err := tree.SaveVersion()
			require.NoError(t, err)

			// Reopen so the fast node cache starts empty, as after a restart, and Get reads fast nodes from the DB.
			tree = NewMutableTree(db, 0, false, NewNopLogger(), tc.opts...)
			_, err = tree.Load()
			require.NoError(t, err)

			previous, err := tree.GetImmutable(1)
			require.NoError(t, err)
			requireFastIterator(t, previous, "the previous version must start on the fast path")

			nextState := maps.Clone(previousState)
			for key, value := range tc.set {
				_, err := tree.Set([]byte(key), []byte(value))
				require.NoError(t, err)
				nextState[key] = value
			}
			for _, key := range tc.remove {
				_, removed, err := tree.Remove([]byte(key))
				require.NoError(t, err)
				require.True(t, removed)
				delete(nextState, key)
			}

			wantPrevious := treeReads{iterated: previousState, fetched: previousState}
			got := tc.readDuringSave(t, db, tree, func() treeReads { return read(previous) })
			require.Equal(t, wantPrevious, got)
			require.Equal(t, int64(2), tree.Version())
			require.Equal(t, wantPrevious, read(previous))

			next, err := tree.GetImmutable(2)
			require.NoError(t, err)
			require.Equal(t, treeReads{iterated: nextState, fetched: nextState}, read(next))
			requireFastIterator(t, next, "the saved version must be back on the fast path")
		})
	}
}

// treeReads holds what a reader saw. Both maps equal the version's state when the reads are consistent.
type treeReads struct {
	iterated map[string]string
	fetched  map[string]string // Get of every key the test uses, missing keys left out
	err      error
}

func requireFastIterator(t *testing.T, tree *ImmutableTree, msg string) {
	t.Helper()
	itr, err := tree.Iterator(nil, nil, true)
	require.NoError(t, err)
	require.IsType(t, &FastIterator{}, itr, msg)
	require.NoError(t, itr.Close())
}

func iterateAll(tree *ImmutableTree) (map[string]string, error) {
	itr, err := tree.Iterator(nil, nil, true)
	if err != nil {
		return nil, err
	}
	defer itr.Close()

	all := map[string]string{}
	for ; itr.Valid(); itr.Next() {
		all[string(itr.Key())] = string(itr.Value())
	}
	return all, itr.Error()
}

// goroutineBlockedIn reports whether some goroutine is in a sync.RWMutex call made from fn.
func goroutineBlockedIn(fn string) bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	for n == len(buf) { // truncated
		buf = make([]byte, 2*len(buf))
		n = runtime.Stack(buf, true)
	}
	buf = buf[:n]
	for _, stack := range bytes.Split(buf, []byte("\n\n")) {
		if bytes.Contains(stack, []byte(fn)) && bytes.Contains(stack, []byte("sync.(*RWMutex)")) {
			return true
		}
	}
	return false
}

type iteratorHookDB struct {
	corestore.KVStoreWithBatch
	// beforeFastIterator and afterFastIterator run once, around the opening of the next fast node iterator.
	beforeFastIterator, afterFastIterator atomic.Pointer[func()]
}

func (db *iteratorHookDB) Iterator(start, end []byte) (corestore.Iterator, error) {
	if !bytes.HasPrefix(start, fastKeyFormat.Key()) {
		return db.KVStoreWithBatch.Iterator(start, end)
	}
	runOnce(&db.beforeFastIterator)
	itr, err := db.KVStoreWithBatch.Iterator(start, end)
	runOnce(&db.afterFastIterator)
	return itr, err
}

func runOnce(hook *atomic.Pointer[func()]) {
	if fn := hook.Swap(nil); fn != nil {
		(*fn)()
	}
}
