package iavl

import (
	"github.com/pkg/errors"

	cmn "github.com/tendermint/tmlibs/common"
	dbm "github.com/tendermint/tmlibs/db"
)

type IAVLPersistentTree struct {
	*IAVLTree
	orphans map[string]uint64
}

func NewIAVLPersistentTree(t *IAVLTree) *IAVLPersistentTree {
	return &IAVLPersistentTree{
		IAVLTree: t,
		orphans:  map[string]uint64{},
	}
}

func (t *IAVLPersistentTree) deleteOrphan(hash []byte) (version uint64, deleted bool) {
	if version, ok := t.orphans[string(hash)]; ok {
		delete(t.orphans, string(hash))
		return version, true
	}
	return 0, false
}

func (t *IAVLPersistentTree) loadOrphans(version uint64) {
	t.ndb.traverseOrphansVersion(version, func(k, v []byte) {
		t.orphans[string(v)] = version
	})
}

type IAVLVersionedTree struct {
	// The current (latest) version of the tree.
	*IAVLPersistentTree

	versions map[uint64]*IAVLPersistentTree
	ndb      *nodeDB
}

func NewIAVLVersionedTree(cacheSize int, db dbm.DB) *IAVLVersionedTree {
	ndb := newNodeDB(cacheSize, db)
	head := &IAVLTree{ndb: ndb}

	return &IAVLVersionedTree{
		IAVLPersistentTree: NewIAVLPersistentTree(head),
		versions:           map[uint64]*IAVLPersistentTree{},
		ndb:                ndb,
	}
}

func (tree *IAVLVersionedTree) String() string {
	return tree.ndb.String()
}

func (tree *IAVLVersionedTree) Set(key, value []byte) bool {
	orphaned, updated := tree.IAVLTree.set(key, value)
	tree.addOrphans(orphaned)
	return updated
}

func (tree *IAVLVersionedTree) Remove(key []byte) ([]byte, bool) {
	val, orphaned, removed := tree.IAVLTree.Remove(key)
	tree.addOrphans(orphaned)
	return val, removed
}

func (tree *IAVLVersionedTree) Load() error {
	roots, err := tree.ndb.getRoots()
	if err != nil {
		return err
	}

	var latest uint64
	for _, root := range roots {
		t := NewIAVLPersistentTree(&IAVLTree{ndb: tree.ndb})
		t.Load(root)

		version := t.root.version
		tree.versions[version] = t

		if version > latest {
			latest = version
		}
		t.loadOrphans(version)
	}
	tree.IAVLTree = tree.versions[latest].Copy()

	return nil
}

func (tree *IAVLVersionedTree) GetVersion(key []byte, version uint64) (
	index int, value []byte, exists bool,
) {
	if t, ok := tree.versions[version]; ok {
		return t.Get(key)
	}
	return -1, nil, false
}

func (tree *IAVLVersionedTree) DeleteVersion(version uint64) error {
	if t, ok := tree.versions[version]; ok {
		// TODO: Use version parameter.
		tree.ndb.DeleteOrphans(t.root.version)
		tree.ndb.DeleteRoot(t.root.version)
		tree.ndb.Commit()

		// TODO: Not necessary.
		t.root.leftNode = nil
		t.root.rightNode = nil

		delete(tree.versions, version)

		return nil
	}
	// TODO: What happens if you delete HEAD?
	return errors.Errorf("version %d does not exist", version)
}

func (tree *IAVLVersionedTree) SaveVersion(version uint64) error {
	if _, ok := tree.versions[version]; ok {
		return errors.Errorf("version %d was already saved", version)
	}
	if tree.root == nil {
		return errors.New("tree is empty")
	}
	if version == 0 {
		return errors.New("version must be greater than zero")
	}
	tree.versions[version] = tree.IAVLPersistentTree

	tree.ndb.SaveBranch(tree.root, version, func(hash []byte) {
		tree.deleteOrphan(hash)

		for _, t := range tree.versions {
			if version, ok := t.deleteOrphan(hash); ok {
				tree.ndb.Unorphan(hash, version)
			}
		}
	})
	tree.ndb.SaveRoot(tree.root)
	tree.ndb.SaveOrphans(tree.orphans)
	tree.ndb.Commit()
	tree.IAVLPersistentTree = NewIAVLPersistentTree(tree.Copy())

	return nil
}

func (tree *IAVLVersionedTree) addOrphans(orphans []*IAVLNode) {
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
