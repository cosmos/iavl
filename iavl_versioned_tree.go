package iavl

import (
	dbm "github.com/tendermint/tmlibs/db"
)

type IAVLVersionedTree struct {
	// TODO: Should be roots.
	versions map[uint64]*IAVLTree
	head     *IAVLTree
}

func NewIAVLVersionedTree(cacheSize int, db dbm.DB) *IAVLVersionedTree {
	head := NewIAVLTree(cacheSize, db)

	return &IAVLVersionedTree{
		versions: map[uint64]*IAVLTree{},
		head:     head,
	}
}

func (tree *IAVLVersionedTree) GetVersion(key []byte, version uint64) (
	index int, value []byte, exists bool,
) {
	if t, ok := tree.versions[version]; ok {
		return t.Get(key)
	}
	return -1, nil, false
}

func (tree *IAVLVersionedTree) Get(key []byte) (
	index int, value []byte, exists bool,
) {
	return tree.head.Get(key)
}

func (tree *IAVLVersionedTree) Set(key, val []byte) {
	tree.head.Set(key, val)
}

func (tree *IAVLVersionedTree) Remove(key []byte) {
	tree.head.Remove(key)
}

func (tree *IAVLVersionedTree) SaveVersion(version uint64) error {
	tree.head.Save()
	tree.versions[version] = tree.head
	tree.head = tree.head.Copy()

	return nil
}
