package iavl

import (
	"fmt"
	"strings"

	. "github.com/tendermint/tmlibs/common"
	dbm "github.com/tendermint/tmlibs/db"

	"github.com/pkg/errors"
)

/*
Immutable AVL Tree (wraps the Node root)
This tree is not goroutine safe.
*/
type IAVLTree struct {
	root *IAVLNode
	ndb  *nodeDB
}

// NewIAVLTree creates both im-memory and persistent instances
func NewIAVLTree(cacheSize int, db dbm.DB) *IAVLTree {
	if db == nil {
		// In-memory IAVLTree
		return &IAVLTree{}
	} else {
		// Persistent IAVLTree
		ndb := newNodeDB(cacheSize, db)
		return &IAVLTree{
			ndb: ndb,
		}
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
		PanicSanity("It is unsafe to Copy() an unpersisted tree.")
	} else if t.ndb == nil && t.root.hash == nil {
		// An in-memory IAVLTree is finalized when the hashes are
		// calculated.
		t.root.hashWithCount(t)
	}
	return &IAVLTree{
		root: t.root,
		ndb:  t.ndb,
	}
}

func (t *IAVLTree) Size() int {
	if t.root == nil {
		return 0
	}
	return t.root.size
}

func (t *IAVLTree) Height() int8 {
	if t.root == nil {
		return 0
	}
	return t.root.height
}

func (t *IAVLTree) Has(key []byte) bool {
	if t.root == nil {
		return false
	}
	return t.root.has(t, key)
}

func (t *IAVLTree) Set(key []byte, value []byte) (updated bool) {
	if t.root == nil {
		t.root = NewIAVLNode(key, value)
		return false
	}
	t.root, updated = t.root.set(t, key, value)
	return updated
}

// BatchSet adds a Set to the current batch, will get handled atomically
func (t *IAVLTree) BatchSet(key []byte, value []byte) {
	t.ndb.batch.Set(key, value)
}

func (t *IAVLTree) Hash() []byte {
	if t.root == nil {
		return nil
	}
	hash, _ := t.root.hashWithCount(t)
	return hash
}

func (t *IAVLTree) HashWithCount() ([]byte, int) {
	if t.root == nil {
		return nil, 0
	}
	return t.root.hashWithCount(t)
}

func (t *IAVLTree) Save() []byte {
	if t.root == nil {
		return nil
	}
	if t.ndb != nil {
		t.root.save(t)
		t.ndb.Commit()
	}
	return t.root.hash
}

// Sets the root node by reading from db.
// If the hash is empty, then sets root to nil.
func (t *IAVLTree) Load(hash []byte) {
	if len(hash) == 0 {
		t.root = nil
	} else {
		t.root = t.ndb.GetNode(hash)
	}
}

// Get returns the index and value of the specified key if it exists, or nil
// and the next index, if it doesn't.
func (t *IAVLTree) Get(key []byte) (index int, value []byte, exists bool) {
	if t.root == nil {
		return 0, nil, false
	}
	return t.root.get(t, key)
}

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

func (t *IAVLTree) Remove(key []byte) (value []byte, removed bool) {
	if t.root == nil {
		return nil, false
	}
	newRootHash, newRoot, _, value, removed := t.root.remove(t, key)
	if !removed {
		return nil, false
	}
	if newRoot == nil && newRootHash != nil {
		t.root = t.ndb.GetNode(newRootHash)
	} else {
		t.root = newRoot
	}
	return value, true
}

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
