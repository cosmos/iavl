package iavl

import (
	cmn "github.com/tendermint/tmlibs/common"
)

// orphaningTree is a tree which keeps track of orphaned nodes.
type orphaningTree struct {
	*Tree

	// A map of orphan hash to orphan version.
	// The version stored here is the one at which the orphan's lifetime
	// begins.
	orphans map[string]int64
}

// newOrphaningTree creates a new orphaning tree from the given *Tree.
func newOrphaningTree(t *Tree) *orphaningTree {
	return &orphaningTree{
		Tree:    t,
		orphans: map[string]int64{},
	}
}

// Set a key on the underlying tree while storing the orphaned nodes.
func (tree *orphaningTree) Set(key, value []byte) bool {
	orphaned, updated := tree.Tree.set(key, value)
	tree.addOrphans(orphaned)
	return updated
}

// Remove a key from the underlying tree while storing the orphaned nodes.
func (tree *orphaningTree) Remove(key []byte) ([]byte, bool) {
	val, orphaned, removed := tree.Tree.remove(key)
	tree.addOrphans(orphaned)
	return val, removed
}

// Unorphan undoes the orphaning of a node, removing the orphan entry on disk
// if necessary.
func (tree *orphaningTree) unorphan(hash []byte) {
	tree.deleteOrphan(hash)
	tree.ndb.Unorphan(hash)
}

// SaveAs saves the underlying Tree and assigns it a new version.
// Saves orphans too.
func (tree *orphaningTree) SaveAs(version int64) {
	if tree.root == nil {
		// There can still be orphans, for example if the root is the node being
		// removed.
		tree.ndb.SaveOrphans(tree.version, tree.orphans)
		tree.ndb.SaveEmptyRoot(version)
	} else {
		// Save the current tree. For each saved node, we delete any existing
		// orphan entries in the previous trees.  This is necessary because
		// sometimes tree re-balancing causes nodes to be incorrectly marked as
		// orphaned, since tree patterns after a re-balance may mirror previous
		// tree patterns, with matching hashes.
		tree.ndb.SaveBranch(tree.root, func(node *Node) {
			tree.unorphan(node._hash())
		})
		tree.ndb.SaveOrphans(tree.version, tree.orphans)
		tree.ndb.SaveRoot(tree.root, version)
	}
	tree.ndb.Commit()
}

// Add orphans to the orphan list. Doesn't write to disk.
func (tree *orphaningTree) addOrphans(orphans []*Node) {
	for _, node := range orphans {
		if !node.persisted {
			// We don't need to orphan nodes that were never persisted.
			continue
		}
		if len(node.hash) == 0 {
			cmn.PanicSanity("Expected to find node hash, but was empty")
		}
		tree.orphans[string(node.hash)] = node.version
	}
}

// Delete an orphan from the orphan list. Doesn't write to disk.
func (tree *orphaningTree) deleteOrphan(hash []byte) (version int64, deleted bool) {
	if version, ok := tree.orphans[string(hash)]; ok {
		delete(tree.orphans, string(hash))
		return version, true
	}
	return 0, false
}
