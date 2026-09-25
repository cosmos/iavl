package iavl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	corestore "cosmossdk.io/core/store"
	dbm "github.com/cosmos/iavl/db"
	"github.com/stretchr/testify/require"
)

// interruptedCommitDB returns an error after an intermediate batch of version
// two has actually been written. This leaves the database in the same state as
// a process stopped before the root for version two was committed.
type interruptedCommitDB struct {
	corestore.KVStoreWithBatch
	armed    bool
	snapshot *dbm.MemDB
}

func (db *interruptedCommitDB) NewBatch() corestore.Batch { return db.NewBatchWithSize(0) }
func (db *interruptedCommitDB) NewBatchWithSize(size int) corestore.Batch {
	return &interruptedCommitBatch{Batch: db.KVStoreWithBatch.NewBatchWithSize(size), db: db}
}

type interruptedCommitBatch struct {
	corestore.Batch
	db         *interruptedCommitDB
	node, root bool
}

func (b *interruptedCommitBatch) Set(key, value []byte) error {
	if len(key) == 13 && key[0] == 's' && binary.BigEndian.Uint64(key[1:9]) == 2 {
		b.node = true
		b.root = b.root || binary.BigEndian.Uint32(key[9:]) == 1
	}
	return b.Batch.Set(key, value)
}

func (b *interruptedCommitBatch) Write() error {
	if err := b.Batch.Write(); err != nil {
		return err
	}
	if err := b.captureSnapshot(); err != nil {
		return err
	}
	return nil
}

func (b *interruptedCommitBatch) WriteSync() error {
	if err := b.Batch.WriteSync(); err != nil {
		return err
	}
	if err := b.captureSnapshot(); err != nil {
		return err
	}
	return nil
}

func (b *interruptedCommitBatch) captureSnapshot() error {
	if !b.db.armed || !b.node || b.root || b.db.snapshot != nil {
		return nil
	}
	itr, err := b.db.KVStoreWithBatch.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	snapshot := dbm.NewMemDB()
	for ; itr.Valid(); itr.Next() {
		if err := snapshot.Set(bytes.Clone(itr.Key()), bytes.Clone(itr.Value())); err != nil {
			return err
		}
	}
	if err := itr.Error(); err != nil {
		return err
	}
	b.db.snapshot = snapshot
	return nil
}

func TestReplayAfterIncompleteVersion(t *testing.T) {
	for _, skipFastStorage := range []bool{true, false} {
		t.Run(fmt.Sprintf("skipFastStorage=%t", skipFastStorage), func(t *testing.T) {
			// A tiny threshold makes SaveVersion flush node data before the root.
			underlying := dbm.NewMemDB()
			faultDB := &interruptedCommitDB{KVStoreWithBatch: underlying}
			newTree := func(db corestore.KVStoreWithBatch) *MutableTree {
				return NewMutableTree(db, 0, skipFastStorage, NewNopLogger(), FlushThresholdOption(1000))
			}
			tree := newTree(faultDB)
			_, err := tree.Set([]byte("initial"), []byte("retained"))
			require.NoError(t, err)
			_, version, err := tree.SaveVersion()
			require.NoError(t, err)
			require.EqualValues(t, 1, version)

			apply := func(tree *MutableTree) {
				for i := 0; i < 100; i++ {
					_, err := tree.Set([]byte(fmt.Sprintf("key-%03d", i)), bytes.Repeat([]byte{byte(i)}, 128))
					require.NoError(t, err)
				}
			}
			apply(tree)
			faultDB.armed = true
			_, _, err = tree.SaveVersion()
			require.NoError(t, err)
			require.NotNil(t, faultDB.snapshot, "an intermediate node batch must be flushed")
			faultDB.armed = false
			// Reopen the database image captured just after that flush.
			snapshot := faultDB.snapshot
			faultDB = &interruptedCommitDB{KVStoreWithBatch: snapshot}

			// The application reloads its previous committed height for replay.
			replayed := newTree(faultDB)
			_, err = replayed.LoadVersion(1)
			require.NoError(t, err)
			require.False(t, replayed.VersionExists(2))
			apply(replayed)
			_, version, err = replayed.SaveVersion()
			require.NoError(t, err)
			require.EqualValues(t, 2, version)

			loaded := newTree(snapshot)
			_, err = loaded.LoadVersion(2)
			require.NoError(t, err)
			value, err := loaded.Get([]byte("initial"))
			require.NoError(t, err)
			require.Equal(t, []byte("retained"), value)
			for i := 0; i < 100; i++ {
				value, err = loaded.Get([]byte(fmt.Sprintf("key-%03d", i)))
				require.NoError(t, err)
				require.Equal(t, bytes.Repeat([]byte{byte(i)}, 128), value)
			}
		})
	}
}
