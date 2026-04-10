package iavl

import (
	"fmt"
	"testing"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
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

func tableSchema(t *testing.T, conn *sqlite3.Conn, tableName string) string {
	t.Helper()
	stmt, err := conn.Prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?")
	require.NoError(t, err)
	defer stmt.Close()

	require.NoError(t, stmt.Bind(tableName))
	hasRow, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, hasRow)

	var sql string
	require.NoError(t, stmt.Scan(&sql))
	return sql
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
	rootSchema := tableSchema(t, broken.sql.treeWrite, RootTableName)
	require.NoError(t, broken.sql.treeWrite.Exec(fmt.Sprintf("DROP TABLE %s", RootTableName)))

	_, version, err := mt.SaveVersionConcurrently()
	require.Error(t, err)
	require.Equal(t, int64(1), version)

	require.NoError(t, broken.sql.treeWrite.Exec(rootSchema))

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
		fmt.Sprintf("CREATE TABLE %s (x int)",
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
