package iavl

import (
	cmn "github.com/tendermint/tmlibs/common"
)

// OrphaningTree is a tree which keeps track of orphaned nodes.
type OrphaningTree struct {
	*IAVLTree

	// A map of orphan hash to orphan version.
	orphans map[string]uint64
}

// NewOrphaningTree creates a new orphaning tree from the given *IAVLTree.
func NewOrphaningTree(t *IAVLTree) *OrphaningTree {
	return &OrphaningTree{
		IAVLTree: t,
		orphans:  map[string]uint64{},
	}
}

// Set a key on the underlying tree while storing the orphaned nodes.
func (tree *OrphaningTree) Set(key, value []byte) bool {
	orphaned, updated := tree.IAVLTree.set(key, value)
	tree.addOrphans(orphaned)
	return updated
}

// Remove a key from the underlying tree while storing the orphaned nodes.
func (tree *OrphaningTree) Remove(key []byte) ([]byte, bool) {
	val, orphaned, removed := tree.IAVLTree.Remove(key)
	tree.addOrphans(orphaned)
	return val, removed
}

// Load the tree from disk, from the given root hash, including all orphans.
func (tree *OrphaningTree) Load(root []byte) {
	tree.IAVLTree.Load(root)
	tree.loadOrphans(tree.root.version)
}

// Unorphan undoes the orphaning of a node, removing the orphan entry on disk
// if necessary.
func (tree *OrphaningTree) Unorphan(hash []byte, version uint64) {
	if version, ok := tree.deleteOrphan(hash); ok {
		tree.ndb.Unorphan(hash, version)
	}
}

// Save the underlying IAVLTree. Saves orphans too.
func (tree *OrphaningTree) Save(fn func(*IAVLNode) *IAVLNode) {
	tree.ndb.SaveBranch(tree.root, func(node *IAVLNode) *IAVLNode {
		// Ensure that nodes saved to disk aren't later orphaned.
		tree.deleteOrphan(node.hash)
		return fn(node)
	})
	tree.ndb.SaveOrphans(tree.orphans)
}

// Load orphans from disk.
func (tree *OrphaningTree) loadOrphans(version uint64) {
	tree.ndb.traverseOrphansVersion(version, func(k, v []byte) {
		tree.orphans[string(v)] = version
	})
}

// Add orphans to the orphan list. Doesn't write to disk.
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

// Delete an orphan from the orphan list. Doesn't write to disk.
func (tree *OrphaningTree) deleteOrphan(hash []byte) (version uint64, deleted bool) {
	if version, ok := tree.orphans[string(hash)]; ok {
		delete(tree.orphans, string(hash))
		return version, true
	}
	return 0, false
}
