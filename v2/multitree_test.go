package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSaveVersionConcurrentlySingleError(t *testing.T) {
	tmpDir := t.TempDir()
	mt := NewMultiTree(NewTestLogger(), tmpDir, DefaultTreeOptions())

	require.NoError(t, mt.MountTree("a"))
	require.NoError(t, mt.MountTree("b"))

	for _, tree := range mt.Trees {
		_, err := tree.Set([]byte("k"), []byte("v"))
		require.NoError(t, err)
	}

	// Break one tree so that only one goroutine returns an error.
	broken := mt.Trees["b"]
	require.NotNil(t, broken)
	require.NoError(t, broken.sql.treeWrite.Exec("DROP TABLE root"))

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
