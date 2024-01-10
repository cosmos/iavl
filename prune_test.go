package iavl

import (
	"testing"

	"github.com/cosmos/iavl/v2/testutil"
	"github.com/stretchr/testify/require"
)

func TestTraverseOrphans(t *testing.T) {
	pool := NewNodePool()
	sql, err := NewSqliteDb(pool, defaultSqliteDbOptions(SqliteDbOptions{Path: t.TempDir()}))
	require.NoError(t, err)
	tree := NewTree(sql, pool, TreeOptions{CheckpointInterval: 1})

	opts := testutil.NewTreeBuildOptions()
	itr := opts.Iterator
	orphans := make(map[NodeKey]bool)
	fn := func(node *Node) error {
		orphans[node.nodeKey] = true
		return nil
	}

	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		version := itr.Version()
		if version > 10 {
			break
		}

		nodes := itr.Nodes()
		for ; nodes.Valid(); err = nodes.Next() {
			require.NoError(t, err)
			node := nodes.GetNode()
			if node.Delete {
				_, _, err = tree.Remove(node.Key)
				require.NoError(t, err)
			} else {
				_, err = tree.Set(node.Key, node.Value)
				require.NoError(t, err)
			}
		}
		tempOrphans := tree.leafOrphans
		tempOrphans = append(tempOrphans, tree.branchOrphans...)
		deletedNodesCount := len(tree.deletes)
		_, v, err := tree.SaveVersion()
		require.NoError(t, err)
		require.Equal(t, version, v)

		if v > 1 {
			orphans = make(map[NodeKey]bool)
			err = tree.traverseOrphans(v-1, v, fn)
			require.NoError(t, err)
			require.Equal(t, len(tempOrphans)+deletedNodesCount, len(orphans))
			for _, nodeKey := range tempOrphans {
				require.True(t, orphans[nodeKey])
			}
		}
	}
}
