package iavl

import (
	"github.com/pkg/errors"

	cmn "github.com/tendermint/tmlibs/common"
	dbm "github.com/tendermint/tmlibs/db"
)

type IAVLVersionedTree struct {
	// The current (latest) version of the tree.
	*IAVLOrphaningTree

	versions map[uint64]*IAVLOrphaningTree
	ndb      *nodeDB
}

func NewIAVLVersionedTree(cacheSize int, db dbm.DB) *IAVLVersionedTree {
	ndb := newNodeDB(cacheSize, db)
	head := &IAVLTree{ndb: ndb}

	return &IAVLVersionedTree{
		IAVLOrphaningTree: NewIAVLOrphaningTree(head),
		versions:          map[uint64]*IAVLOrphaningTree{},
		ndb:               ndb,
	}
}

func (tree *IAVLVersionedTree) String() string {
	return tree.ndb.String()
}

func (tree *IAVLVersionedTree) Load() error {
	roots, err := tree.ndb.getRoots()
	if err != nil {
		return err
	}

	var latest uint64
	for _, root := range roots {
		t := NewIAVLOrphaningTree(&IAVLTree{ndb: tree.ndb})
		t.Load(root)

		version := t.root.version
		tree.versions[version] = t

		if version > latest {
			latest = version
		}
	}
	tree.IAVLTree = tree.versions[latest].Copy()

	return nil
}

func (tree *IAVLVersionedTree) GetVersioned(key []byte, version uint64) (
	index int, value []byte, exists bool,
) {
	if t, ok := tree.versions[version]; ok {
		return t.Get(key)
	}
	return -1, nil, false
}

func (tree *IAVLVersionedTree) DeleteVersion(version uint64) error {
	if t, ok := tree.versions[version]; ok {
		if version != t.root.version {
			cmn.PanicSanity("Version being saved is not the same as root")
		}
		tree.ndb.DeleteOrphans(version)
		tree.ndb.DeleteRoot(version)
		tree.ndb.Commit()

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
	tree.versions[version] = tree.IAVLOrphaningTree

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
	tree.IAVLOrphaningTree = NewIAVLOrphaningTree(tree.Copy())

	return nil
}
