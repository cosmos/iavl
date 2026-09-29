package iavl

import (
	"sync"
	"testing"
	"time"

	dbm "github.com/cosmos/iavl/db"
	"github.com/stretchr/testify/require"
)

func setupNodeDB(t *testing.T) *nodeDB {
	t.Helper()
	db := dbm.NewMemDB()
	return newNodeDB(db, 0, DefaultOptions(), NewNopLogger())
}

func TestCommittingFlags(t *testing.T) {
	ndb := setupNodeDB(t)

	require.False(t, ndb.IsCommitting())

	ndb.SetCommitting()
	require.True(t, ndb.IsCommitting())

	ndb.UnsetCommitting()
	require.False(t, ndb.IsCommitting())

	select {
	case <-ndb.chCommitting:
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected signal on chCommitting")
	}
}

func TestCommittingSignalRelease(t *testing.T) {
	ndb := setupNodeDB(t)
	ndb.SetCommitting()

	signaled := make(chan bool)
	go func() {
		<-ndb.chCommitting
		signaled <- true
	}()

	time.Sleep(50 * time.Millisecond)
	require.True(t, ndb.IsCommitting())

	select {
	case <-signaled:
		t.Fatal("signal received too early")
	case <-time.After(50 * time.Millisecond):
		// ok
	}

	ndb.UnsetCommitting()

	select {
	case <-signaled:
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected signal after UnsetCommitting")
	}
}

func TestDeleteFromPruningDuringCommit(t *testing.T) {
	ndb := setupNodeDB(t)
	dummyKey := []byte("key-to-delete")
	ndb.SetCommitting()

	done := make(chan struct{})
	go func() {
		err := ndb.deleteFromPruning(dummyKey)
		require.NoError(t, err)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("deleteFromPruning should block during commit")
	default:
		// expected
	}

	ndb.UnsetCommitting()

	select {
	case <-done:
		// success
	case <-time.After(200 * time.Millisecond):
		t.Fatal("deleteFromPruning did not resume after UnsetCommitting")
	}
}

func TestSaveNodeFromPruningDuringCommit(t *testing.T) {
	ndb := setupNodeDB(t)
	node := &Node{
		key:           []byte("key"),
		value:         []byte("val"),
		size:          1,
		subtreeHeight: 0,
		nodeKey:       &NodeKey{version: 1, nonce: 0},
		hash:          []byte("hash"),
	}

	ndb.SetCommitting()
	done := make(chan struct{})
	go func() {
		err := ndb.saveNodeFromPruning(node)
		require.NoError(t, err)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("saveNodeFromPruning should block during commit")
	default:
		// expected
	}

	ndb.UnsetCommitting()

	select {
	case <-done:
		// success
	case <-time.After(200 * time.Millisecond):
		t.Fatal("saveNodeFromPruning did not resume after UnsetCommitting")
	}
}

func TestConcurrentCommitAndPruningAccess(t *testing.T) {
	ndb := setupNodeDB(t)
	var wg sync.WaitGroup

	// Commit simulation
	wg.Add(1)
	go func() {
		defer wg.Done()
		ndb.SetCommitting()
		time.Sleep(100 * time.Millisecond) // simulate commit duration
		ndb.UnsetCommitting()
	}()

	// Pruning access simulation
	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()
		err := ndb.deleteFromPruning([]byte("key1"))
		require.NoError(t, err)
		elapsed := time.Since(start)
		require.GreaterOrEqual(t, elapsed.Milliseconds(), int64(100), "should wait until commit is done")
	}()

	// Simultaneous state check
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		require.True(t, ndb.IsCommitting())
	}()

	wg.Wait()
}
