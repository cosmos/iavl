package iavl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestMultiTree(t *testing.T, storeKeys ...string) *MultiTree {
	t.Helper()
	tmpDir := t.TempDir()
	mt := NewMultiTree(NewTestLogger(), tmpDir, DefaultTreeOptions())

	for _, sk := range storeKeys {
		require.NoError(t, mt.MountTree(sk))
	}

	for _, tree := range mt.Trees {
		_, err := tree.Set([]byte("k"), []byte("v"))
		require.NoError(t, err)
	}
	return mt
}

func createRootTable(t *testing.T, tree *Tree) {
	t.Helper()
	require.NoError(t, tree.sql.treeWrite.Exec(fmt.Sprintf(`CREATE TABLE %s (
version int,
node_version int,
node_sequence int,
bytes blob,
checkpoint bool,
PRIMARY KEY (version))`, RootTableName)))
}

func TestSaveVersionConcurrentlySingleError(t *testing.T) {
	mt := newTestMultiTree(t, "a", "b")

	// Break one tree so that only one goroutine returns an error.
	broken := mt.Trees["b"]
	require.NotNil(t, broken)
	require.NoError(t, broken.sql.treeWrite.Exec(fmt.Sprintf("DROP TABLE %s", RootTableName)))

	_, version, err := mt.SaveVersionConcurrently()
	require.Error(t, err)
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		require.Len(t, joined.Unwrap(), 1)
	} else {
		t.Fatalf("expected joined error")
	}
	require.Equal(t, int64(1), version)

	require.NoError(t, mt.Close())
}

func TestSaveVersionConcurrentlySequentialCalls(t *testing.T) {
	mt := newTestMultiTree(t, "a", "b")

	broken := mt.Trees["b"]
	require.NotNil(t, broken)
	require.NoError(t, broken.sql.treeWrite.Exec(fmt.Sprintf("DROP TABLE %s", RootTableName)))

	_, version, err := mt.SaveVersionConcurrently()
	require.Error(t, err)
	require.Equal(t, int64(1), version)

	createRootTable(t, broken)

	_, version, err = mt.SaveVersionConcurrently()
	require.NoError(t, err)
	require.Equal(t, int64(2), version)

	require.NoError(t, mt.Close())
}

func TestSaveVersionConcurrentlyAllErrors(t *testing.T) {
	mt := newTestMultiTree(t, "a", "b")

	for _, tree := range mt.Trees {
		require.NoError(t, tree.sql.treeWrite.Exec(fmt.Sprintf("DROP TABLE %s", RootTableName)))
	}

	_, version, err := mt.SaveVersionConcurrently()
	require.Error(t, err)
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		require.Len(t, joined.Unwrap(), len(mt.Trees))
	} else {
		t.Fatalf("expected joined error")
	}
	require.Equal(t, int64(-1), version)

	require.NoError(t, mt.Close())
}

func TestSnapshotConcurrentlyError(t *testing.T) {
	mt := newTestMultiTree(t, "a", "b")

	broken := mt.Trees["b"]
	require.NotNil(t, broken)
	require.NoError(t, broken.sql.leafWrite.Exec(
		fmt.Sprintf("CREATE TABLE %s (ordinal int, version int, sequence int, bytes blob)",
			SnapshotTableName(0),
		),
	))

	err := mt.SnapshotConcurrently()
	require.Error(t, err)
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		require.Len(t, joined.Unwrap(), 1)
	} else {
		t.Fatalf("expected joined error")
	}

	require.NoError(t, mt.Close())
}

func TestWarmLeavesCollectsAllErrors(t *testing.T) {
	mt := newTestMultiTree(t, "a", "b")

	for _, tree := range mt.Trees {
		require.NoError(t, tree.sql.leafWrite.Exec(fmt.Sprintf("DROP TABLE %s", LeafTableName)))
	}

	err := mt.WarmLeaves()
	require.Error(t, err)
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		require.Len(t, joined.Unwrap(), len(mt.Trees))
	} else {
		t.Fatalf("expected joined error")
	}

	require.NoError(t, mt.Close())
}

func TestSaveVersionConcurrentlyHappyPath(t *testing.T) {
	mt := newTestMultiTree(t, "a", "b")

	_, version, err := mt.SaveVersionConcurrently()
	require.NoError(t, err)
	require.Equal(t, int64(1), version)

	require.NoError(t, mt.Close())
}
