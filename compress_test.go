package iavl

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompressImporter_BranchBeforeLeaves(t *testing.T) {
	importer := NewCompressImporter(&noopImporter{})
	node := &ExportNode{Version: 1, Height: 1}
	require.Error(t, importer.Add(node))
}

func TestCompressImporter_BranchAfterSingleLeaf(t *testing.T) {
	importer := NewCompressImporter(&noopImporter{})

	// delta encoded leaf key with shared=0 prefix
	leafKey := append([]byte{0}, 'a')
	node := &ExportNode{Key: leafKey, Value: []byte{1}, Version: 1, Height: 0}
	require.NoError(t, importer.Add(node))

	err := importer.Add(&ExportNode{Version: 1, Height: 1})
	require.Error(t, err)
}

func TestDeltaDecode_SharedExceedsLastKey(t *testing.T) {
	lastKey := []byte("ab")

	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(lastKey)+1)) //nolint: gosec
	encoded := append(buf[:n], 'c')

	_, err := deltaDecode(encoded, lastKey)
	require.Error(t, err)
}

func TestDeltaDecode_LargeSharedValue(t *testing.T) {
	lastKey := []byte("ab")

	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], 1<<40)
	encoded := append(buf[:n], 'c')

	_, err := deltaDecode(encoded, lastKey)
	require.Error(t, err)
}

// noopImporter is a NodeImporter that records nothing — used to isolate
// CompressImporter behavior from inner importer validation.
type noopImporter struct{}

func (*noopImporter) Add(*ExportNode) error { return nil }
