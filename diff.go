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

func StateChanges(ndb *nodeDB, version int64, root []byte, successorRoot []byte) (*ChangeSet, error) {
	curIter, err := NewNodeIterator(successorRoot, ndb)
	if err != nil {
		return nil, err
	}

	prevIter, err := NewNodeIterator(root, ndb)
	if err != nil {
		return nil, err
	}

	var changeSet []KVPair
	sharedNodes := make(map[string]struct{})
	newKeys := make(map[string]struct{})
	for curIter.Valid() {
		node := curIter.GetNode()
		if node.version <= version {
			sharedNodes[ibytes.UnsafeBytesToStr(node.hash)] = struct{}{}
		} else if node.isLeaf() {
			changeSet = append(changeSet, KVPair{Key: node.key, Value: node.value})
			newKeys[ibytes.UnsafeBytesToStr(node.key)] = struct{}{}
		}
		curIter.Next(node.version <= version)
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
