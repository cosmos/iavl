package iavl

import (
	"bytes"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/iavl/db"
)

func TestReadPreviousVersionDuringSaveVersion(t *testing.T) {
	const timeout = 10 * time.Second

	var (
		removedKey = []byte("b")
		addedKey   = []byte("d")
	)
	previousState := treeReads{all: map[string]string{"a": "1", "b": "1", "c": "1"}, removed: []byte("1")}
	nextState := treeReads{all: map[string]string{"a": "2", "c": "1", "d": "2"}, added: []byte("2")}

	read := func(tree *ImmutableTree) treeReads {
		var got treeReads
		got.all, got.err = iterateAll(tree)
		if got.err != nil {
			return got
		}
		if got.removed, got.err = tree.Get(removedKey); got.err != nil {
			return got
		}
		got.added, got.err = tree.Get(addedKey)
		return got
	}

	testCases := []struct {
		name string
		opts []Option
		// readDuringSave saves the next version and returns what a reader of the previous version saw on the way.
		readDuringSave func(t *testing.T, db *commitHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads
	}{
		{
			name: "fast nodes of the next version are flushed before the commit",
			opts: []Option{FlushThresholdOption(1)},
			readDuringSave: func(t *testing.T, _ *commitHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads {
				// SaveVersion starts here, and the low threshold makes the batch flush every fast node change at once.
				require.NoError(t, tree.saveFastNodeVersion(tree.WorkingVersion()))

				got := readPrevious()
				value, err := tree.GetVersioned(removedKey, tree.Version())
				require.NoError(t, err)
				assert.Equal(t, previousState.removed, value)

				_, _, err = tree.SaveVersion()
				require.NoError(t, err)
				return got
			},
		},
		{
			name: "next version commits between the version check and the fast iterator snapshot",
			readDuringSave: func(t *testing.T, db *commitHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads {
				save := func() {
					_, _, err := tree.SaveVersion()
					require.NoError(t, err)
				}
				db.onFastIterator.Store(&save)

				got := readPrevious()
				require.Nil(t, db.onFastIterator.Load(), "the read opened no fast iterator")
				return got
			},
		},
		{
			name: "version check waits for the batch write",
			readDuringSave: func(t *testing.T, db *commitHookDB, tree *MutableTree, readPrevious func() treeReads) treeReads {
				done := make(chan treeReads, 1)
				// Runs inside the commit, so the reader cannot pass the version check before the batch is written.
				startRead := func() {
					go func() { done <- readPrevious() }()
					require.Eventually(t, func() bool {
						return goroutineBlockedIn("(*nodeDB).getLatestVersion")
					}, timeout, time.Millisecond, "reader never reached the latest version check")
				}
				db.onWrite.Store(&startRead)

				_, _, err := tree.SaveVersion()
				require.NoError(t, err)
				require.Nil(t, db.onWrite.Load(), "SaveVersion wrote no batch")

				select {
				case got := <-done:
					return got
				case <-time.After(timeout):
					require.FailNow(t, "reader did not finish after SaveVersion")
					return treeReads{}
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := &commitHookDB{DB: dbm.NewMemDB()}
			tree := NewMutableTree(db, 0, false, NewNopLogger(), tc.opts...)
			for key, value := range previousState.all {
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
			isFast, err := previous.IsFastCacheEnabled()
			require.NoError(t, err)
			require.True(t, isFast, "the previous version must start on the fast path")

			for key, value := range nextState.all {
				_, err := tree.Set([]byte(key), []byte(value))
				require.NoError(t, err)
			}
			_, removed, err := tree.Remove(removedKey)
			require.NoError(t, err)
			require.True(t, removed)

			got := tc.readDuringSave(t, db, tree, func() treeReads { return read(previous) })
			require.Equal(t, previousState, got)
			require.Equal(t, int64(2), tree.Version())
			require.Equal(t, previousState, read(previous))

			next, err := tree.GetImmutable(2)
			require.NoError(t, err)
			require.Equal(t, nextState, read(next))
			itr, err := next.Iterator(nil, nil, true)
			require.NoError(t, err)
			require.IsType(t, &FastIterator{}, itr, "the saved version must be back on the fast path")
			require.NoError(t, itr.Close())
		})
	}
}

type treeReads struct {
	all     map[string]string
	removed []byte // value of the key the next version removes
	added   []byte // value of the key the next version adds
	err     error
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

// goroutineBlockedIn reports whether some goroutine waits on a sync.RWMutex inside fn.
func goroutineBlockedIn(fn string) bool {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, stack := range bytes.Split(buf, []byte("\n\n")) {
		if bytes.Contains(stack, []byte(fn)) && bytes.Contains(stack, []byte("sync.(*RWMutex)")) {
			return true
		}
	}
	return false
}

type commitHookDB struct {
	dbm.DB
	// onWrite runs once after the next batch write, while nodeDB still holds its lock.
	onWrite atomic.Pointer[func()]
	// onFastIterator runs once before the next fast node iterator opens.
	onFastIterator atomic.Pointer[func()]
}

func (db *commitHookDB) NewBatch() dbm.Batch {
	return &commitHookBatch{Batch: db.DB.NewBatch(), db: db}
}

func (db *commitHookDB) NewBatchWithSize(size int) dbm.Batch {
	return &commitHookBatch{Batch: db.DB.NewBatchWithSize(size), db: db}
}

func (db *commitHookDB) Iterator(start, end []byte) (dbm.Iterator, error) {
	if bytes.HasPrefix(start, fastKeyFormat.Key()) {
		runOnce(&db.onFastIterator)
	}
	return db.DB.Iterator(start, end)
}

type commitHookBatch struct {
	dbm.Batch
	db *commitHookDB
}

func (b *commitHookBatch) Write() error {
	if err := b.Batch.Write(); err != nil {
		return err
	}
	runOnce(&b.db.onWrite)
	return nil
}

func (b *commitHookBatch) WriteSync() error {
	if err := b.Batch.WriteSync(); err != nil {
		return err
	}
	runOnce(&b.db.onWrite)
	return nil
}

func runOnce(hook *atomic.Pointer[func()]) {
	if fn := hook.Swap(nil); fn != nil {
		(*fn)()
	}
}
