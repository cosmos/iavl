package iavl

import (
	dbm "github.com/tendermint/tmlibs/db"
)

type IAVLPersistentTree struct {
	*IAVLTree
	orphans []*IAVLNode
}

func NewIAVLPersistentTree(t *IAVLTree) *IAVLPersistentTree {
	return &IAVLPersistentTree{
		IAVLTree: t,
		orphans:  []*IAVLNode{},
	}
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

func (tree *IAVLVersionedTree) Set(key, value []byte) bool {
	orphaned, removed := tree.IAVLTree.set(key, value)
	tree.orphans = append(tree.orphans, orphaned...)
	return removed
}

func (tree *IAVLVersionedTree) Remove(key []byte) ([]byte, bool) {
	val, orphaned, removed := tree.IAVLTree.Remove(key)
	tree.orphans = append(tree.orphans, orphaned...)
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
		tree.versions[t.root.version] = t

		if t.root.version > latest {
			latest = t.root.version
		}
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

func (tree *IAVLVersionedTree) ReleaseVersion(version uint64) {
	if t, ok := tree.versions[version]; ok {
		// TODO: Use version parameter.
		tree.ndb.DeleteOrphans(t.root.version)
		tree.ndb.DeleteRoot(t.root.version)
		tree.ndb.Commit()

		t.root.leftNode = nil
		t.root.rightNode = nil

		delete(tree.versions, version)
	}
}

func (tree *IAVLVersionedTree) SaveVersion(version uint64) error {
	tree.ndb.SaveOrphans(tree.orphans)
	tree.orphans = nil

	tree.versions[version] = tree.IAVLPersistentTree
	tree.IAVLPersistentTree.SaveAs(version)
	tree.IAVLPersistentTree = NewIAVLPersistentTree(tree.Copy())

	return nil
}
