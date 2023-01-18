package iavl

import (
	"bytes"
	"sort"

	ibytes "github.com/cosmos/iavl/internal/bytes"
)

// ChangeSet represents the state changes extracted from diffing iavl versions.
type ChangeSet struct {
	Pairs []KVPair
}

type KVPair struct {
	Delete bool
	Key    []byte
	Value  []byte
}

// extractStateChanges extracts the state changes by between two versions of the tree.
// it first traverse the `root` tree to find out the `newKeys` and `sharedNodes`,
// `newKeys` are the keys of the newly added leaf nodes, which represents the inserts and updates,
// `sharedNodes` are the referenced nodes that are created in previous versions,
// then we traverse the `prevRoot` tree to find out the deletion entries, we can skip the subtrees
// marked by the `sharedNodes`.
func (ndb *nodeDB) extractStateChanges(prevVersion int64, prevRoot []byte, root []byte) (*ChangeSet, error) {
	curIter, err := NewNodeIterator(root, ndb)
	if err != nil {
		return nil, err
	}

	prevIter, err := NewNodeIterator(prevRoot, ndb)
	if err != nil {
		return nil, err
	}

	var changeSet []KVPair
	sharedNodes := make(map[string]struct{})
	newKeys := make(map[string]struct{})
	for curIter.Valid() {
		node := curIter.GetNode()
		shared := node.version <= prevVersion
		if shared {
			sharedNodes[ibytes.UnsafeBytesToStr(node.hash)] = struct{}{}
		} else if node.isLeaf() {
			changeSet = append(changeSet, KVPair{Key: node.key, Value: node.value})
			newKeys[ibytes.UnsafeBytesToStr(node.key)] = struct{}{}
		}
		// skip subtree of shared nodes
		curIter.Next(shared)
	}
	if err := curIter.Error(); err != nil {
		return nil, err
	}

	for prevIter.Valid() {
		node := prevIter.GetNode()
		_, shared := sharedNodes[ibytes.UnsafeBytesToStr(node.hash)]
		if !shared && node.isLeaf() {
			_, updated := newKeys[ibytes.UnsafeBytesToStr(node.key)]
			if !updated {
				changeSet = append(changeSet, KVPair{Delete: true, Key: node.key})
			}
		}
		prevIter.Next(shared)
	}
	if err := prevIter.Error(); err != nil {
		return nil, err
	}

	sort.Slice(changeSet, func(i, j int) bool {
		return bytes.Compare(changeSet[i].Key, changeSet[j].Key) == -1
	})
	return &ChangeSet{Pairs: changeSet}, nil
}
