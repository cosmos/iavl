package iavl

import (
	"bytes"
	"fmt"

	cmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tendermint/libs/db"
)

// ErrVersionDoesNotExist is returned if a requested version does not exist.
var ErrVersionDoesNotExist = fmt.Errorf("version does not exist")

// MutableTree is a persistent tree which keeps track of versions.
type MutableTree struct {
	*ImmutableTree                          // The current, working tree.
	orphans        map[string]int64         // Nodes removed by changes to working tree
	versions       map[int64]*ImmutableTree // The previous, saved versions of the tree.
	ndb            *nodeDB
}

// NewMutableTree returns a new tree with the specified cache size and datastore.
func NewMutableTree(db dbm.DB, cacheSize int) *MutableTree {
	ndb := newNodeDB(db, cacheSize)
	head := &ImmutableTree{ndb: ndb}

	return &MutableTree{
		ImmutableTree: head,
		orphans:       map[string]int64{},
		versions:      map[int64]*ImmutableTree{},
		ndb:           ndb,
	}
}

// IsEmpty returns whether or not the tree has any keys. Only trees that are
// not empty can be saved.
func (tree *MutableTree) IsEmpty() bool {
	return tree.ImmutableTree.Size() == 0
}

// VersionExists returns whether or not a version exists.
func (tree *MutableTree) VersionExists(version int64) bool {
	_, ok := tree.versions[version]
	return ok
}

// Hash returns the hash of the latest saved version of the tree, as returned
// by SaveVersion. If no versions have been saved, Hash returns nil.
func (tree *MutableTree) Hash() []byte {
	if tree.version > 0 {
		return tree.versions[tree.version].Hash()
	}
	return nil
}

func (tree *MutableTree) WorkingHash() []byte {
	return tree.ImmutableTree.Hash()
}

// String returns a string representation of the tree.
func (tree *MutableTree) String() string {
	return tree.ndb.String()
}

// Set sets a key in the working tree. Nil values are not supported.
func (tree *MutableTree) Set(key, value []byte) bool {
	orphaned, updated := tree.set(key, value)
	tree.addOrphans(orphaned)
	return updated
}

func (t *MutableTree) set(key []byte, value []byte) (orphaned []*Node, updated bool) {
	if value == nil {
		panic(fmt.Sprintf("Attempt to store nil value at key '%s'", key))
	}
	if t.ImmutableTree.root == nil {
		t.ImmutableTree.root = NewNode(key, value, t.version+1)
		return nil, false
	}
	t.ImmutableTree.root, updated, orphaned = t.recursiveSet(t.ImmutableTree.root, key, value)

	return orphaned, updated
}

func (t *MutableTree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, orphaned []*Node,
) {
	version := t.version + 1

	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1:
			return &Node{
				key:       node.key,
				height:    1,
				size:      2,
				leftNode:  NewNode(key, value, version),
				rightNode: node,
				version:   version,
			}, false, []*Node{}
		case 1:
			return &Node{
				key:       key,
				height:    1,
				size:      2,
				leftNode:  node,
				rightNode: NewNode(key, value, version),
				version:   version,
			}, false, []*Node{}
		default:
			return NewNode(key, value, version), true, []*Node{node}
		}
	} else {
		orphaned = append(orphaned, node)
		node = node.clone(version)

		if bytes.Compare(key, node.key) < 0 {
			var leftOrphaned []*Node
			node.leftNode, updated, leftOrphaned = t.recursiveSet(node.getLeftNode(t.ImmutableTree), key, value)
			node.leftHash = nil // leftHash is yet unknown
			orphaned = append(orphaned, leftOrphaned...)
		} else {
			var rightOrphaned []*Node
			node.rightNode, updated, rightOrphaned = t.recursiveSet(node.getRightNode(t.ImmutableTree), key, value)
			node.rightHash = nil // rightHash is yet unknown
			orphaned = append(orphaned, rightOrphaned...)
		}

		if updated {
			return node, updated, orphaned
		}
		node.calcHeightAndSize(t.ImmutableTree)
		newNode, balanceOrphaned := node.balance(t.ImmutableTree)
		return newNode, updated, append(orphaned, balanceOrphaned...)
	}
}

// Remove removes a key from the working tree.
func (tree *MutableTree) Remove(key []byte) ([]byte, bool) {
	val, orphaned, removed := tree.remove(key)
	tree.addOrphans(orphaned)
	return val, removed
}

// remove tries to remove a key from the tree and if removed, returns its
// value, nodes orphaned and 'true'.
func (t *MutableTree) remove(key []byte) (value []byte, orphans []*Node, removed bool) {
	if t.root == nil {
		return nil, nil, false
	}
	newRootHash, newRoot, _, value, orphaned := t.recursiveRemove(t.root, key)
	if len(orphaned) == 0 {
		return nil, nil, false
	}

	if newRoot == nil && newRootHash != nil {
		t.root = t.ndb.GetNode(newRootHash)
	} else {
		t.root = newRoot
	}
	return value, orphaned, true
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
// - the orphaned nodes.
func (t *MutableTree) recursiveRemove(node *Node, key []byte) ([]byte, *Node, []byte, []byte, []*Node) {
	version := t.version + 1

	if node.isLeaf() {
		if bytes.Equal(key, node.key) {
			return nil, nil, nil, node.value, []*Node{node}
		}
		return node.hash, node, nil, nil, nil
	}

	// node.key < key; we go to the left to find the key:
	if bytes.Compare(key, node.key) < 0 {
		newLeftHash, newLeftNode, newKey, value, orphaned := t.recursiveRemove(node.getLeftNode(t.ImmutableTree), key)

		if len(orphaned) == 0 {
			return node.hash, node, nil, value, orphaned
		} else if newLeftHash == nil && newLeftNode == nil { // left node held value, was removed
			return node.rightHash, node.rightNode, node.key, value, orphaned
		}
		orphaned = append(orphaned, node)

		newNode := node.clone(version)
		newNode.leftHash, newNode.leftNode = newLeftHash, newLeftNode
		newNode.calcHeightAndSize(t.ImmutableTree)
		newNode, balanceOrphaned := newNode.balance(t.ImmutableTree)

		return newNode.hash, newNode, newKey, value, append(orphaned, balanceOrphaned...)
	}
	// node.key >= key; either found or look to the right:
	newRightHash, newRightNode, newKey, value, orphaned := t.recursiveRemove(node.getRightNode(t.ImmutableTree), key)

	if len(orphaned) == 0 {
		return node.hash, node, nil, value, orphaned
	} else if newRightHash == nil && newRightNode == nil { // right node held value, was removed
		return node.leftHash, node.leftNode, nil, value, orphaned
	}
	orphaned = append(orphaned, node)

	newNode := node.clone(version)
	newNode.rightHash, newNode.rightNode = newRightHash, newRightNode
	if newKey != nil {
		newNode.key = newKey
	}
	newNode.calcHeightAndSize(t.ImmutableTree)
	newNode, balanceOrphaned := newNode.balance(t.ImmutableTree)

	return newNode.hash, newNode, nil, value, append(orphaned, balanceOrphaned...)
}

// Load the latest versioned tree from disk.
//
// Returns the version number of the latest version found
func (tree *MutableTree) Load() (int64, error) {
	return tree.LoadVersion(0)
}

// Load a versioned tree from disk.
//
// If version is 0, the latest version is loaded.
//
// Returns the version number of the latest version found
func (tree *MutableTree) LoadVersion(targetVersion int64) (int64, error) {
	roots, err := tree.ndb.getRoots()
	if err != nil {
		return 0, err
	}
	if len(roots) == 0 {
		return 0, nil
	}

	// Load all roots from the database.
	latestVersion := int64(0)
	for version, root := range roots {

		// Construct a tree manually.
		t := &ImmutableTree{}
		t.ndb = tree.ndb
		t.version = version
		if len(root) != 0 {
			t.root = tree.ndb.GetNode(root)
		}
		tree.versions[version] = t

		if version > latestVersion &&
			(targetVersion == 0 || version <= targetVersion) {

			latestVersion = version
		}
	}

	// Validate latestVersion
	if !(targetVersion == 0 || latestVersion == targetVersion) {
		return latestVersion, fmt.Errorf("Wanted to load target %v but only found up to %v",
			targetVersion, latestVersion)
	}

	// Set the working tree to a copy of the latest.
	tree.ImmutableTree = tree.versions[latestVersion].clone()
	tree.orphans = map[string]int64{}

	return latestVersion, nil
}

// Rollback resets the working tree to the latest saved version, discarding
// any unsaved modifications.
func (tree *MutableTree) Rollback() {
	if tree.version > 0 {
		tree.ImmutableTree = tree.versions[tree.version].clone()
	} else {
		tree.ImmutableTree = &ImmutableTree{ndb: tree.ndb, version: 0}
	}
	tree.orphans = map[string]int64{}
}

// GetVersioned gets the value at the specified key and version.
func (tree *MutableTree) GetVersioned(key []byte, version int64) (
	index int, value []byte,
) {
	if t, ok := tree.versions[version]; ok {
		return t.Get(key)
	}
	return -1, nil
}

// SaveVersion saves a new tree version to disk, based on the current state of
// the tree. Returns the hash and new version number.
func (tree *MutableTree) SaveVersion() ([]byte, int64, error) {
	version := tree.version + 1

	if _, ok := tree.versions[version]; ok {
		// Same hash means idempotent.  Return success.
		var existingHash = tree.versions[version].Hash()
		var newHash = tree.ImmutableTree.Hash()
		if bytes.Equal(existingHash, newHash) {
			tree.ImmutableTree = tree.versions[version].clone()
			tree.orphans = map[string]int64{}
			return existingHash, version, nil
		}
		return nil, version, fmt.Errorf("version %d was already saved to different hash %X (existing hash %X)",
			version, newHash, existingHash)
	}

	// Persist version and stash to .versions.
	tree.SaveOrphans(version)
	tree.versions[version] = tree.ImmutableTree

	// Set new working tree.
	tree.ImmutableTree = tree.ImmutableTree.clone()
	tree.orphans = map[string]int64{}

	return tree.Hash(), version, nil
}

// DeleteVersion deletes a tree version from disk. The version can then no
// longer be accessed.
func (tree *MutableTree) DeleteVersion(version int64) error {
	if version == 0 {
		return cmn.NewError("version must be greater than 0")
	}
	if version == tree.version {
		return cmn.NewError("cannot delete latest saved version (%d)", version)
	}
	if _, ok := tree.versions[version]; !ok {
		return cmn.ErrorWrap(ErrVersionDoesNotExist, "")
	}

	tree.ndb.DeleteVersion(version)
	tree.ndb.Commit()

	delete(tree.versions, version)

	return nil
}

func (tree *MutableTree) SaveOrphans(version int64) {
	if version != tree.version+1 {
		panic(fmt.Sprintf("Expected to save version %d but tried to save %d", tree.version+1, version))
	}
	if tree.root == nil {
		// There can still be orphans, for example if the root is the node being
		// removed.
		debug("SAVE EMPTY TREE %v\n", version)
		tree.ndb.SaveOrphans(version, tree.orphans)
		tree.ndb.SaveEmptyRoot(version)
	} else {
		debug("SAVE TREE %v\n", version)
		// Save the current tree.
		tree.ndb.SaveBranch(tree.root)
		tree.ndb.SaveOrphans(version, tree.orphans)
		tree.ndb.SaveRoot(tree.root, version)
	}
	tree.ndb.Commit()
	tree.version = version
}

func (tree *MutableTree) addOrphans(orphans []*Node) {
	for _, node := range orphans {
		if !node.persisted {
			// We don't need to orphan nodes that were never persisted.
			continue
		}
		if len(node.hash) == 0 {
			panic("Expected to find node hash, but was empty")
		}
		tree.orphans[string(node.hash)] = node.version
	}
}
