package iavl

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

func BenchmarkImport(b *testing.B) {
	tree := setupExportTreeSized(b, 4096)
	exported := make([]*ExportNode, 0, 4096)
	exporter := tree.Export()
	for {
		item, err := exporter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(b, err)
		exported = append(exported, item)
	}
	exporter.Close()

	for n := 0; n < b.N; n++ {
		newTree, err := NewMutableTree(db.NewMemDB(), 0)
		require.NoError(b, err)
		importer, err := NewImporter(newTree, tree.Version())
		require.NoError(b, err)
		for _, item := range exported {
			err = importer.Add(item)
			require.NoError(b, err)
		}
		err = importer.Commit()
		require.NoError(b, err)
	}
}
