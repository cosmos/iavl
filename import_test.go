package iavl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/cosmos/cosmos-db"
)

func ExampleImporter() {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	if err != nil {
		// handle err
	}

	_, err = tree.Set([]byte("a"), []byte{1})
	if err != nil {
		// handle err
	}

	_, err = tree.Set([]byte("b"), []byte{2})
	if err != nil {
		// handle err
	}
	_, err = tree.Set([]byte("c"), []byte{3})
	if err != nil {
		// handle err
	}
	_, version, err := tree.SaveVersion()
	if err != nil {
		// handle err
	}

	itree, err := tree.GetImmutable(version)
	if err != nil {
		// handle err
	}
	exporter, err := itree.Export(PostOrderTraverse)
	if err != nil {
		// handle err
	}
	defer exporter.Close()
	exported := []*ExportNode{}
	for {
		var node *ExportNode
		node, err = exporter.Next()
		if err == ErrorExportDone {
			break
		} else if err != nil {
			// handle err
		}
		exported = append(exported, node)
	}

	newTree, err := NewMutableTree(db.NewMemDB(), 0, false)
	if err != nil {
		// handle err
	}
	importer, err := newTree.Import(version, PostOrderTraverse)
	if err != nil {
		// handle err
	}
	defer importer.Close()
	for _, node := range exported {
		err = importer.Add(node)
		if err != nil {
			// handle err
		}
	}
	err = importer.Commit()
	if err != nil {
		// handle err
	}
}

func TestImporter_NegativeVersion(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	require.NoError(t, err)
	_, err = tree.Import(-1, PostOrderTraverse)
	require.Error(t, err)
}

func TestImporter_NotEmpty(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	require.NoError(t, err)
	_, err = tree.Set([]byte("a"), []byte{1})
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	_, err = tree.Import(1, PostOrderTraverse)
	require.Error(t, err)
}

func TestImporter_NotEmptyDatabase(t *testing.T) {
	db := db.NewMemDB()

	tree, err := NewMutableTree(db, 0, false)
	require.NoError(t, err)
	_, err = tree.Set([]byte("a"), []byte{1})
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	tree, err = NewMutableTree(db, 0, false)
	require.NoError(t, err)
	_, err = tree.Load()
	require.NoError(t, err)

	_, err = tree.Import(1, PostOrderTraverse)
	require.Error(t, err)
}

func TestImporter_NotEmptyUnsaved(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	require.NoError(t, err)
	_, err = tree.Set([]byte("a"), []byte{1})
	require.NoError(t, err)

	_, err = tree.Import(1, PostOrderTraverse)
	require.Error(t, err)
}

func TestImporter_Add(t *testing.T) {
	k := []byte("key")
	v := []byte("value")

	testcases := map[string]struct {
		node  *ExportNode
		valid bool
	}{
		"nil node":          {nil, false},
		"valid":             {&ExportNode{Key: k, Value: v, Version: 1, Height: 0}, true},
		"no key":            {&ExportNode{Key: nil, Value: v, Version: 1, Height: 0}, false},
		"no value":          {&ExportNode{Key: k, Value: nil, Version: 1, Height: 0}, false},
		"version too large": {&ExportNode{Key: k, Value: v, Version: 2, Height: 0}, false},
		"no version":        {&ExportNode{Key: k, Value: v, Version: 0, Height: 0}, false},
		// further cases will be handled by Node.validate()
	}
	for desc, tc := range testcases {
		tc := tc // appease scopelint
		t.Run(desc, func(t *testing.T) {
			for _, orderType := range []OrderType{PreOrderTraverse, PostOrderTraverse} {
				tree, err := NewMutableTree(db.NewMemDB(), 0, false)
				require.NoError(t, err)
				var importer *Importer
				importer, err = tree.Import(1, orderType)
				require.NoError(t, err)
				defer importer.Close()
				if orderType == PreOrderTraverse {
					require.NoError(t, importer.Add(&ExportNode{Key: k, Value: v, Version: 1, Height: 1}))
					require.NoError(t, importer.Add(&ExportNode{Key: k, Value: v, Version: 1, Height: 0}))
				}
				err = importer.Add(tc.node)
				if tc.valid {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
		})
	}
}

func TestImporter_Add_Closed(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	require.NoError(t, err)
	importer, err := tree.Import(1, PostOrderTraverse)
	require.NoError(t, err)

	importer.Close()
	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.Error(t, err)
	require.Equal(t, ErrNoImport, err)
}

func TestImporter_Close(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	require.NoError(t, err)
	importer, err := tree.Import(1, PostOrderTraverse)
	require.NoError(t, err)

	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.NoError(t, err)

	importer.Close()
	has, err := tree.Has([]byte("key"))
	require.NoError(t, err)
	require.False(t, has)

	importer.Close()
}

func TestImporter_Commit(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	require.NoError(t, err)
	importer, err := tree.Import(1, PostOrderTraverse)
	require.NoError(t, err)

	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.NoError(t, err)

	err = importer.Commit()
	require.NoError(t, err)
	has, err := tree.Has([]byte("key"))
	require.NoError(t, err)
	require.True(t, has)
}

func TestImporter_Commit_Closed(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	require.NoError(t, err)
	importer, err := tree.Import(1, PreOrderTraverse)
	require.NoError(t, err)

	err = importer.Add(&ExportNode{Key: []byte("key"), Value: []byte("value"), Version: 1, Height: 0})
	require.NoError(t, err)

	importer.Close()
	err = importer.Commit()
	require.Error(t, err)
	require.Equal(t, ErrNoImport, err)
}

func TestImporter_Commit_Empty(t *testing.T) {
	tree, err := NewMutableTree(db.NewMemDB(), 0, false)
	require.NoError(t, err)
	importer, err := tree.Import(3, PostOrderTraverse)
	require.NoError(t, err)
	defer importer.Close()

	err = importer.Commit()
	require.NoError(t, err)
	assert.EqualValues(t, 3, tree.Version())
}

func BenchmarkImport(b *testing.B) {
	tree := setupExportTreeSized(b, 4096)
	b.ResetTimer()
	for _, desc := range []string{"Pre Order", "Post Order"} {
		b.Run(fmt.Sprintf("%s Import", desc), func(sub *testing.B) {
			sub.StopTimer()
			var orderType OrderType
			switch desc {
			case "Pre Order":
				orderType = PreOrderTraverse
			case "Post Order":
				orderType = PostOrderTraverse
			}
			exported := make([]*ExportNode, 0, 4096)
			exporter, err := tree.Export(orderType)
			require.NoError(sub, err)
			for {
				item, err := exporter.Next()
				if err == ErrorExportDone {
					break
				} else if err != nil {
					sub.Error(err)
				}
				exported = append(exported, item)
			}
			exporter.Close()
			sub.StartTimer()

			for n := 0; n < sub.N; n++ {
				newTree, err := NewMutableTree(db.NewMemDB(), 0, false)
				require.NoError(sub, err)
				importer, err := newTree.Import(tree.Version(), orderType)
				require.NoError(sub, err)
				for _, item := range exported {
					err = importer.Add(item)
					if err != nil {
						sub.Error(err)
					}
				}
				err = importer.Commit()
				require.NoError(sub, err)
			}
		})
	}
}
