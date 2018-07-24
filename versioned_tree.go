package iavl

import (
	"bytes"
	"fmt"

	cmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tendermint/libs/db"
)

// ErrVersionDoesNotExist is returned if a requested version does not exist.
var ErrVersionDoesNotExist = fmt.Errorf("version does not exist")

// VersionedTree is a persistent tree which keeps track of versions.
type VersionedTree struct {
	*Tree                     // The current, working tree.
	orphans  map[string]int64 // Nodes removed by changes to working tree
	versions map[int64]*Tree  // The previous, saved versions of the tree.
	ndb      *nodeDB
}

// NewVersionedTree returns a new tree with the specified cache size and datastore.
func NewVersionedTree(db dbm.DB, cacheSize int) *VersionedTree {
	ndb := newNodeDB(db, cacheSize)
	head := &Tree{ndb: ndb}

	return &VersionedTree{
		Tree:     head,
		orphans:  map[string]int64{},
		versions: map[int64]*Tree{},
		ndb:      ndb,
	}
}

// IsEmpty returns whether or not the tree has any keys. Only trees that are
// not empty can be saved.
func (tree *VersionedTree) IsEmpty() bool {
	return tree.Tree.Size() == 0
}

// VersionExists returns whether or not a version exists.
func (tree *VersionedTree) VersionExists(version int64) bool {
	_, ok := tree.versions[version]
	return ok
}

// Hash returns the hash of the latest saved version of the tree, as returned
// by SaveVersion. If no versions have been saved, Hash returns nil.
func (tree *VersionedTree) Hash() []byte {
	if tree.version > 0 {
		return tree.versions[tree.version].Hash()
	}
	return nil
}

func (tree *VersionedTree) WorkingHash() []byte {
	return tree.Tree.Hash()
}

// String returns a string representation of the tree.
func (tree *VersionedTree) String() string {
	return tree.ndb.String()
}

// Set sets a key in the working tree. Nil values are not supported.
func (tree *VersionedTree) Set(key, value []byte) bool {
	orphaned, updated := tree.set(key, value)
	tree.addOrphans(orphaned)
	return updated
}

func (t *VersionedTree) set(key []byte, value []byte) (orphaned []*Node, updated bool) {
	if value == nil {
		panic(fmt.Sprintf("Attempt to store nil value at key '%s'", key))
	}
	if t.Tree.root == nil {
		t.Tree.root = NewNode(key, value, t.version+1)
		return nil, false
	}
	t.Tree.root, updated, orphaned = t.recursiveSet(t.Tree.root, key, value)

	return orphaned, updated
}

func (t *VersionedTree) recursiveSet(node *Node, key []byte, value []byte) (
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
			node.leftNode, updated, leftOrphaned = t.recursiveSet(node.getLeftNode(t.Tree), key, value)
			node.leftHash = nil // leftHash is yet unknown
			orphaned = append(orphaned, leftOrphaned...)
		} else {
			var rightOrphaned []*Node
			node.rightNode, updated, rightOrphaned = t.recursiveSet(node.getRightNode(t.Tree), key, value)
			node.rightHash = nil // rightHash is yet unknown
			orphaned = append(orphaned, rightOrphaned...)
		}

		if updated {
			return node, updated, orphaned
		}
		node.calcHeightAndSize(t.Tree)
		newNode, balanceOrphaned := node.balance(t.Tree)
		return newNode, updated, append(orphaned, balanceOrphaned...)
	}
}

// Remove removes a key from the working tree.
func (tree *VersionedTree) Remove(key []byte) ([]byte, bool) {
	val, orphaned, removed := tree.Tree.remove(key)
	tree.addOrphans(orphaned)
	return val, removed
}

// Load the latest versioned tree from disk.
//
// Returns the version number of the latest version found
func (tree *VersionedTree) Load() (int64, error) {
	return tree.LoadVersion(0)
}

// Load a versioned tree from disk.
//
// If version is 0, the latest version is loaded.
//
// Returns the version number of the latest version found
func (tree *VersionedTree) LoadVersion(targetVersion int64) (int64, error) {
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
		t := &Tree{}
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
	tree.Tree = tree.versions[latestVersion].clone()
	tree.orphans = map[string]int64{}

	return latestVersion, nil
}

// Rollback resets the working tree to the latest saved version, discarding
// any unsaved modifications.
func (tree *VersionedTree) Rollback() {
	if tree.version > 0 {
		tree.Tree = tree.versions[tree.version].clone()
	} else {
		tree.Tree = &Tree{ndb: tree.ndb, version: 0}
	}
	tree.orphans = map[string]int64{}
}

// GetVersioned gets the value at the specified key and version.
func (tree *VersionedTree) GetVersioned(key []byte, version int64) (
	index int, value []byte,
) {
	if t, ok := tree.versions[version]; ok {
		return t.Get(key)
	}
	return -1, nil
}

// SaveVersion saves a new tree version to disk, based on the current state of
// the tree. Returns the hash and new version number.
func (tree *VersionedTree) SaveVersion() ([]byte, int64, error) {
	version := tree.version + 1

	if _, ok := tree.versions[version]; ok {
		// Same hash means idempotent.  Return success.
		var existingHash = tree.versions[version].Hash()
		var newHash = tree.Tree.Hash()
		if bytes.Equal(existingHash, newHash) {
			tree.Tree = tree.versions[version].clone()
			tree.orphans = map[string]int64{}
			return existingHash, version, nil
		}
		return nil, version, fmt.Errorf("version %d was already saved to different hash %X (existing hash %X)",
			version, newHash, existingHash)
	}

	// Persist version and stash to .versions.
	tree.SaveOrphans(version)
	tree.versions[version] = tree.Tree

	// Set new working tree.
	tree.Tree = tree.Tree.clone()
	tree.orphans = map[string]int64{}

	return tree.Hash(), version, nil
}

// DeleteVersion deletes a tree version from disk. The version can then no
// longer be accessed.
func (tree *VersionedTree) DeleteVersion(version int64) error {
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

func (tree *VersionedTree) SaveOrphans(version int64) {
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

func (tree *VersionedTree) addOrphans(orphans []*Node) {
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
