package iavl

import (
	cmn "github.com/tendermint/tmlibs/common"
	dbm "github.com/tendermint/tmlibs/db"
)

type IAVLPersistentTree struct {
	*IAVLTree
	orphans map[string]*IAVLNode
}

func NewIAVLPersistentTree(t *IAVLTree) *IAVLPersistentTree {
	return &IAVLPersistentTree{
		IAVLTree: t,
		orphans:  map[string]*IAVLNode{},
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

func (tree *IAVLVersionedTree) String() string {
	return tree.ndb.String()
}

func (tree *IAVLVersionedTree) Set(key, value []byte) bool {
	orphaned, removed := tree.IAVLTree.set(key, value)
	tree.addOrphans(orphaned)
	return removed
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

	// TODO: Load orphans too? They are needed when unorphaning.

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

		// TODO: Not necessary.
		t.root.leftNode = nil
		t.root.rightNode = nil

		delete(tree.versions, version)
	}
	// TODO: What happens if you release HEAD?
}

func (tree *IAVLVersionedTree) SaveVersion(version uint64) error {
	// TODO: Don't allow saving over an existing version.

	tree.versions[version] = tree.IAVLPersistentTree
	tree.IAVLPersistentTree.SaveAs(version, func(hash []byte) {
		if _, ok := tree.orphans[string(hash)]; ok {
			delete(tree.orphans, string(hash))
		}
		for _, t := range tree.versions {
			// TODO: Check old orphans on disk?
			// Could call Unorphan() for all hashes, silently skips not-founds.
			if n, ok := t.orphans[string(hash)]; ok {
				tree.ndb.Unorphan(n.version, hash)
				delete(t.orphans, string(hash))
			}
		}
	})

	tree.ndb.SaveOrphans(tree.orphans)
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
		tree.orphans[string(node.hash)] = node
	}
}
