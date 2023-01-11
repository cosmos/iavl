package iavl

import (
	"bytes"
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

	var (
		// current shared node between two versions
		sharedNode *Node
		// record all newly added leaf nodes in newer version, it represents all updates and insertions.
		newLeafNodes []*Node
		// orphaned leaf nodes in previous version, which represents all deletions and updates.
		// both `newLeafNodes` and `orphanedLeafNodes` are ordered by key.
		orphanedLeafNodes []*Node
	)

	advanceSharedNode := func() {
		// Forward `curIter` until the next `sharedNode`.
		// `sharedNode` will be `nil` if the new version is exhausted.
		sharedNode = nil
		for curIter.Valid() {
			node := curIter.GetNode()
			shared := node.version <= prevVersion
			curIter.Next(shared)
			if shared {
				sharedNode = node
				break
			} else if node.isLeaf() {
				newLeafNodes = append(newLeafNodes, node)
			}
		}
	}
	advanceSharedNode()

	// Traverse `prevIter` to find orphaned nodes in the previous version.
	for prevIter.Valid() {
		node := prevIter.GetNode()
		shared := sharedNode != nil && (node == sharedNode || bytes.Equal(node.hash, sharedNode.hash))
		// skip sub-tree of shared nodes
		prevIter.Next(shared)
		if shared {
			advanceSharedNode()
		} else if node.isLeaf() {
			orphanedLeafNodes = append(orphanedLeafNodes, node)
		}
	}

	findDeletedNodes(orphanedLeafNodes, newLeafNodes, func(node *Node, deleted bool) {
		var pair KVPair
		pair.Key = node.key
		if deleted {
			pair.Delete = true
		} else {
			pair.Value = node.value
		}
		changeSet = append(changeSet, pair)
	})

	return &ChangeSet{Pairs: changeSet}, nil
}

// findDeletedNodes find out the deleted keys in `nodes1`.
// Invariant: both `nodes1` and `nodes2` are ordered by key.
func findDeletedNodes(nodes1 []*Node, nodes2 []*Node, cb func(node *Node, deleted bool)) {
	// find out the deletions by diff two list of ordered nodes
	var i1, i2 int
	for {
		if i1 >= len(nodes1) {
			// insertions
			for ; i2 < len(nodes2); i2++ {
				cb(nodes2[i2], false)
			}
			break
		}

		if i2 >= len(nodes2) {
			// deletions
			for ; i1 < len(nodes1); i1++ {
				cb(nodes1[i1], true)
			}
			break
		}

		cur1 := nodes1[i1]
		cur2 := nodes2[i2]

		switch bytes.Compare(cur1.key, cur2.key) {
		case -1:
			// deletion
			cb(cur1, true)
			i1++
		case 1:
			// insertion
			cb(cur2, false)
			i2++
		default:
			// update
			cb(cur2, false)
			i1++
			i2++
		}
	}

}
