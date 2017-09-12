package iavl

import (
	cmn "github.com/tendermint/tmlibs/common"
)

type OrphaningTree struct {
	*IAVLTree
	orphans map[string]uint64
}

func NewOrphaningTree(t *IAVLTree) *OrphaningTree {
	return &OrphaningTree{
		IAVLTree: t,
		orphans:  map[string]uint64{},
	}
}

func (tree *OrphaningTree) Set(key, value []byte) bool {
	orphaned, updated := tree.IAVLTree.set(key, value)
	tree.addOrphans(orphaned)
	return updated
}

func (tree *OrphaningTree) Remove(key []byte) ([]byte, bool) {
	val, orphaned, removed := tree.IAVLTree.Remove(key)
	tree.addOrphans(orphaned)
	return val, removed
}

func (tree *OrphaningTree) Load(root []byte) {
	tree.IAVLTree.Load(root)
	tree.loadOrphans(tree.root.version)
}

func (tree *OrphaningTree) loadOrphans(version uint64) {
	tree.ndb.traverseOrphansVersion(version, func(k, v []byte) {
		tree.orphans[string(v)] = version
	})
}

func (tree *OrphaningTree) addOrphans(orphans []*IAVLNode) {
	for _, node := range orphans {
		if !node.persisted {
			continue
		}
		if len(node.hash) == 0 {
			cmn.PanicSanity("Expected to find node hash, but was empty")
		}
		tree.orphans[string(node.hash)] = node.version
	}
}

func (tree *OrphaningTree) deleteOrphan(hash []byte) (version uint64, deleted bool) {
	if version, ok := tree.orphans[string(hash)]; ok {
		delete(tree.orphans, string(hash))
		return version, true
	}
	return 0, false
}
