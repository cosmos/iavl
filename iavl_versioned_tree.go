package iavl

import (
	dbm "github.com/tendermint/tmlibs/db"
)

type IAVLVersionedTree struct {
	Head *IAVLTree

	// TODO: Should be roots.
	versions map[uint64]*IAVLTree
	ndb      *nodeDB
}

func NewIAVLVersionedTree(cacheSize int, db dbm.DB) *IAVLVersionedTree {
	ndb := newNodeDB(cacheSize, db)
	head := &IAVLTree{ndb: ndb}

	return &IAVLVersionedTree{
		versions: map[uint64]*IAVLTree{},
		Head:     head,
		ndb:      ndb,
	}
}

func (tree *IAVLVersionedTree) Load() error {
	roots, err := tree.ndb.getRoots()
	if err != nil {
		return err
	}

	var latest uint64
	for _, root := range roots {
		t := &IAVLTree{ndb: tree.ndb}
		t.Load(root)
		tree.versions[t.root.version] = t

		if t.root.version > latest {
			latest = t.root.version
		}
	}
	tree.Head = tree.versions[latest].Copy()

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
	if _, ok := tree.versions[version]; ok {
		tree.versions[version].Release()
		delete(tree.versions, version)
	}
}

func (tree *IAVLVersionedTree) Get(key []byte) (
	index int, value []byte, exists bool,
) {
	return tree.Head.Get(key)
}

func (tree *IAVLVersionedTree) Set(key, val []byte) {
	tree.Head.Set(key, val)
}

func (tree *IAVLVersionedTree) Remove(key []byte) {
	tree.Head.Remove(key)
}

func (tree *IAVLVersionedTree) SaveVersion(version uint64) error {
	tree.Head.SaveAs(version)
	tree.versions[version] = tree.Head
	tree.Head = tree.Head.Copy()

	return nil
}
