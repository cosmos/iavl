package iavl

import (
	"fmt"
	"strings"

	cmn "github.com/tendermint/tmlibs/common"
	dbm "github.com/tendermint/tmlibs/db"

	"github.com/pkg/errors"
)

// IAVLTree is an immutable AVL+ Tree. Note that this tree is not thread-safe.
type IAVLTree struct {
	root *IAVLNode
	ndb  *nodeDB
}

// NewIAVLTree creates both im-memory and persistent instances
func NewIAVLTree(cacheSize int, db dbm.DB) *IAVLTree {
	if db == nil {
		// In-memory IAVLTree.
		return &IAVLTree{}
	}
	return &IAVLTree{
		// NodeDB-backed IAVLTree.
		ndb: newNodeDB(cacheSize, db),
	}
}

// String returns a string representation of IAVLTree.
func (t *IAVLTree) String() string {
	leaves := []string{}
	t.Iterate(func(key []byte, val []byte) (stop bool) {
		leaves = append(leaves, fmt.Sprintf("%x: %x", key, val))
		return false
	})
	return "IAVLTree{" + strings.Join(leaves, ", ") + "}"
}

// DEPRECATED. Please use iavl.VersionedTree instead if you need to hold
// references to multiple tree versions.
//
// Copy returns a copy of the tree.
// The returned tree and the original tree are goroutine independent.
// That is, they can each run in their own goroutine.
// However, upon Save(), any other trees that share a db will become
// outdated, as some nodes will become orphaned.
// Note that Save() clears leftNode and rightNode.  Otherwise,
// two copies would not be goroutine independent.
func (t *IAVLTree) Copy() *IAVLTree {
	if t.root == nil {
		return &IAVLTree{
			root: nil,
			ndb:  t.ndb,
		}
	}
	if t.ndb != nil && !t.root.persisted {
		// Saving a tree finalizes all the nodes.
		// It sets all the hashes recursively,
		// clears all the leftNode/rightNode values recursively,
		// and all the .persisted flags get set.
		cmn.PanicSanity("It is unsafe to Copy() an unpersisted tree.")
	} else if t.ndb == nil && t.root.hash == nil {
		// An in-memory IAVLTree is finalized when the hashes are
		// calculated.
		t.root.hashWithCount()
	}
	return &IAVLTree{
		root: t.root,
		ndb:  t.ndb,
	}
}

// Size returns the number of leaf nodes in the tree.
func (t *IAVLTree) Size() int {
	if t.root == nil {
		return 0
	}
	return t.root.size
}

// Height returns the height of the tree.
func (t *IAVLTree) Height() int8 {
	if t.root == nil {
		return 0
	}
	return t.root.height
}

// Has returns whether or not a key exists.
func (t *IAVLTree) Has(key []byte) bool {
	if t.root == nil {
		return false
	}
	return t.root.has(t, key)
}

// Set a key.
func (t *IAVLTree) Set(key []byte, value []byte) (updated bool) {
	_, updated = t.set(key, value)
	return updated
}

func (t *IAVLTree) set(key []byte, value []byte) (orphaned []*IAVLNode, updated bool) {
	if t.root == nil {
		t.root = NewIAVLNode(key, value)
		return nil, false
	}
	t.root, updated, orphaned = t.root.set(t, key, value)

	return orphaned, updated
}

// BatchSet adds a Set to the current batch, will get handled atomically
func (t *IAVLTree) BatchSet(key []byte, value []byte) {
	t.ndb.batch.Set(key, value)
}

// Hash returns the root hash.
func (t *IAVLTree) Hash() []byte {
	if t.root == nil {
		return nil
	}
	hash, _ := t.root.hashWithCount()
	return hash
}

// hashWithCount returns the root hash and hash count.
func (t *IAVLTree) hashWithCount() ([]byte, int) {
	if t.root == nil {
		return nil, 0
	}
	return t.root.hashWithCount()
}

// Get returns the index and value of the specified key if it exists, or nil
// and the next index, if it doesn't.
func (t *IAVLTree) Get(key []byte) (index int, value []byte, exists bool) {
	if t.root == nil {
		return 0, nil, false
	}
	return t.root.get(t, key)
}

// GetByIndex gets the key and value at the specified index.
func (t *IAVLTree) GetByIndex(index int) (key []byte, value []byte) {
	if t.root == nil {
		return nil, nil
	}
	return t.root.getByIndex(t, index)
}

// GetWithProof gets the value under the key if it exists, or returns nil.
// A proof of existence or absence is returned alongside the value.
func (t *IAVLTree) GetWithProof(key []byte) ([]byte, KeyProof, error) {
	value, eproof, err := t.getWithProof(key)
	if err == nil {
		return value, eproof, nil
	}

	aproof, err := t.keyAbsentProof(key)
	if err == nil {
		return nil, aproof, nil
	}
	return nil, nil, errors.Wrap(err, "could not construct any proof")
}

// GetRangeWithProof gets key/value pairs within the specified range and limit. To specify a descending
// range, swap the start and end keys.
//
// Returns a list of keys, a list of values and a proof.
func (t *IAVLTree) GetRangeWithProof(startKey []byte, endKey []byte, limit int) ([][]byte, [][]byte, *KeyRangeProof, error) {
	return t.getRangeWithProof(startKey, endKey, limit)
}

// GetFirstInRangeWithProof gets the first key/value pair in the specified range, with a proof.
func (t *IAVLTree) GetFirstInRangeWithProof(startKey, endKey []byte) ([]byte, []byte, *KeyFirstInRangeProof, error) {
	return t.getFirstInRangeWithProof(startKey, endKey)
}

// GetLastInRangeWithProof gets the last key/value pair in the specified range, with a proof.
func (t *IAVLTree) GetLastInRangeWithProof(startKey, endKey []byte) ([]byte, []byte, *KeyLastInRangeProof, error) {
	return t.getLastInRangeWithProof(startKey, endKey)
}

// Remove tries to remove a key from the tree and if removed, returns its
// value, and 'true'.
func (t *IAVLTree) Remove(key []byte) ([]byte, bool) {
	value, _, removed := t.remove(key)
	return value, removed
}

// remove tries to remove a key from the tree and if removed, returns its
// value, nodes orphaned and 'true'.
func (t *IAVLTree) remove(key []byte) (value []byte, orphans []*IAVLNode, removed bool) {
	if t.root == nil {
		return nil, nil, false
	}
	newRootHash, newRoot, _, value, orphaned := t.root.remove(t, key)
	if len(orphaned) == 0 {
		return nil, nil, false
	}

	if newRoot == nil && newRootHash != nil {
		t.root = t.ndb.GetNode(newRootHash)
	} else {
		t.root = newRoot
	}
	return value, orphaned, true
}

// Iterate iterates over all keys of the tree, in order.
func (t *IAVLTree) Iterate(fn func(key []byte, value []byte) bool) (stopped bool) {
	if t.root == nil {
		return false
	}
	return t.root.traverse(t, true, func(node *IAVLNode) bool {
		if node.height == 0 {
			return fn(node.key, node.value)
		} else {
			return false
		}
	})
}

// IterateRange makes a callback for all nodes with key between start and end non-inclusive.
// If either are nil, then it is open on that side (nil, nil is the same as Iterate)
func (t *IAVLTree) IterateRange(start, end []byte, ascending bool, fn func(key []byte, value []byte) bool) (stopped bool) {
	if t.root == nil {
		return false
	}
	return t.root.traverseInRange(t, start, end, ascending, false, func(node *IAVLNode) bool {
		if node.height == 0 {
			return fn(node.key, node.value)
		} else {
			return false
		}
	})
}

// IterateRangeInclusive makes a callback for all nodes with key between start and end inclusive.
// If either are nil, then it is open on that side (nil, nil is the same as Iterate)
func (t *IAVLTree) IterateRangeInclusive(start, end []byte, ascending bool, fn func(key []byte, value []byte) bool) (stopped bool) {
	if t.root == nil {
		return false
	}
	return t.root.traverseInRange(t, start, end, ascending, true, func(node *IAVLNode) bool {
		if node.height == 0 {
			return fn(node.key, node.value)
		} else {
			return false
		}
	})
}

// nodeSize is like Size, but includes inner nodes too.
func (t *IAVLTree) nodeSize() int {
	size := 0
	t.root.traverse(t, true, func(n *IAVLNode) bool {
		size++
		return false
	})
	return size
}
