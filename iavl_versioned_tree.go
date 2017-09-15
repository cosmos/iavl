package iavl

import (
	"github.com/pkg/errors"

	cmn "github.com/tendermint/tmlibs/common"
	dbm "github.com/tendermint/tmlibs/db"
)

var ErrVersionDoesNotExist = errors.New("version does not exist")

// VersionedTree is a persistent tree which keeps track of versions.
type VersionedTree struct {
	// The current, latest version of the tree.
	*orphaningTree

	// The previous, saved versions of the tree.
	versions map[uint64]*orphaningTree
	latest   uint64
	ndb      *nodeDB
}

// NewVersionedTree returns a new tree with the specified cache size and datastore.
func NewVersionedTree(cacheSize int, db dbm.DB) *VersionedTree {
	ndb := newNodeDB(cacheSize, db)
	head := &IAVLTree{ndb: ndb}

	return &VersionedTree{
		orphaningTree: newOrphaningTree(head),
		versions:      map[uint64]*orphaningTree{},
		ndb:           ndb,
	}
}

func (tree *VersionedTree) Tree() *IAVLTree {
	return tree.orphaningTree.IAVLTree
}

// String returns a string representation of the tree.
func (tree *VersionedTree) String() string {
	return tree.ndb.String()
}

// Load a versioned tree from disk. All tree versions are loaded automatically.
func (tree *VersionedTree) Load() error {
	roots, err := tree.ndb.getRoots()
	if err != nil {
		return err
	}

	// Load all roots from the database.
	for _, root := range roots {
		t := newOrphaningTree(&IAVLTree{ndb: tree.ndb})
		t.Load(root)

		version := t.rootVersion
		tree.versions[version] = t

		if version > tree.latest {
			tree.latest = version
		}
	}
	// Set the current version to a copy of the latest.
	tree.orphaningTree = newOrphaningTree(tree.versions[tree.latest].Copy())

	return nil
}

// GetVersioned gets the value at the specified key and version.
func (tree *VersionedTree) GetVersioned(key []byte, version uint64) (
	index int, value []byte, exists bool,
) {
	if t, ok := tree.versions[version]; ok {
		return t.Get(key)
	}
	return -1, nil, false
}

// SaveVersion saves a new tree version to disk, based on the current state of
// the tree. Multiple calls to SaveVersion with the same version are not allowed.
func (tree *VersionedTree) SaveVersion(version uint64) error {
	if _, ok := tree.versions[version]; ok {
		return errors.Errorf("version %d was already saved", version)
	}
	if tree.root == nil {
		return errors.New("tree is empty")
	}
	if version == 0 {
		return errors.New("version must be greater than zero")
	}
	if version <= tree.latest {
		return errors.New("version must be greater than latest")
	}
	tree.latest = version
	tree.versions[version] = tree.orphaningTree

	// Save the current tree at the given version. For each saved node, we
	// delete any existing orphan entries in the previous trees.
	// This is necessary because sometimes tree re-balancing causes nodes to be
	// incorrectly marked as orphaned, since tree patterns after a re-balance
	// may mirror previous tree patterns, with matching hashes.
	tree.orphaningTree.SaveVersion(version, func(node *IAVLNode) *IAVLNode {
		for v, t := range tree.versions {
			t.Unorphan(node.hash, v)
		}
		node.version = version

		return node
	})

	tree.ndb.SaveRoot(tree.root)
	tree.ndb.Commit()
	tree.orphaningTree = newOrphaningTree(tree.Copy())

	return nil
}

// DeleteVersion deletes a tree version from disk. The version can then no
// longer be accessed.
func (tree *VersionedTree) DeleteVersion(version uint64) error {
	if version == tree.latest {
		return errors.New("cannot delete current version")
	}
	if t, ok := tree.versions[version]; ok {
		if version != t.root.version {
			cmn.PanicSanity("Version being saved is not the same as root")
		}
		tree.ndb.DeleteVersion(version)
		tree.ndb.Commit()

		delete(tree.versions, version)

		return nil
	}
	return ErrVersionDoesNotExist
}

// GetVersionedWithProof gets the value under the key at the specified version
// if it exists, or returns nil.  A proof of existence or absence is returned
// alongside the value.
func (tree *VersionedTree) GetVersionedWithProof(key []byte, version uint64) ([]byte, KeyProof, error) {
	if t, ok := tree.versions[version]; ok {
		return t.GetWithProof(key)
	}
	return nil, nil, ErrVersionDoesNotExist
}

// GetVersionedRangeWithProof gets key/value pairs within the specified range
// and limit. To specify a descending range, swap the start and end keys.
//
// Returns a list of keys, a list of values and a proof.
func (tree *VersionedTree) GetVersionedRangeWithProof(startKey, endKey []byte, limit int, version uint64) ([][]byte, [][]byte, *KeyRangeProof, error) {
	if t, ok := tree.versions[version]; ok {
		return t.GetRangeWithProof(startKey, endKey, limit)
	}
	return nil, nil, nil, ErrVersionDoesNotExist
}

// GetVersionedFirstInRangeWithProof gets the first key/value pair in the
// specified range, with a proof.
func (tree *VersionedTree) GetVersionedFirstInRangeWithProof(startKey, endKey []byte, version uint64) ([]byte, []byte, *KeyFirstInRangeProof, error) {
	if t, ok := tree.versions[version]; ok {
		return t.GetFirstInRangeWithProof(startKey, endKey)
	}
	return nil, nil, nil, ErrVersionDoesNotExist
}

// GetVersionedLastInRangeWithProof gets the last key/value pair in the
// specified range, with a proof.
func (tree *VersionedTree) GetVersionedLastInRangeWithProof(startKey, endKey []byte, version uint64) ([]byte, []byte, *KeyLastInRangeProof, error) {
	if t, ok := tree.versions[version]; ok {
		return t.GetLastInRangeWithProof(startKey, endKey)
	}
	return nil, nil, nil, ErrVersionDoesNotExist
}
