package iavl

import (
	"fmt"
	"strings"

	dbm "github.com/tendermint/tendermint/libs/db"
)

// Tree is a container for an immutable AVL+ Tree. Changes are performed by
// swapping the internal root with a new one, while the container is mutable.
// Note that this tree is not thread-safe.
type Tree struct {
	root    *Node
	ndb     *nodeDB
	version int64
}

// NewTree creates both in-memory and persistent instances
func NewTree(db dbm.DB, cacheSize int) *Tree {
	if db == nil {
		// In-memory Tree.
		return &Tree{}
	}
	return &Tree{
		// NodeDB-backed Tree.
		ndb: newNodeDB(db, cacheSize),
	}
}

// String returns a string representation of Tree.
func (t *Tree) String() string {
	leaves := []string{}
	t.Iterate(func(key []byte, val []byte) (stop bool) {
		leaves = append(leaves, fmt.Sprintf("%x: %x", key, val))
		return false
	})
	return "Tree{" + strings.Join(leaves, ", ") + "}"
}

// Size returns the number of leaf nodes in the tree.
func (t *Tree) Size() int {
	return int(t.Size64())
}

func (t *Tree) Size64() int64 {
	if t.root == nil {
		return 0
	}
	return t.root.size
}

// Version returns the version of the tree.
func (t *Tree) Version() int {
	return int(t.Version64())
}

func (t *Tree) Version64() int64 {
	return t.version
}

// Height returns the height of the tree.
func (t *Tree) Height() int {
	return int(t.Height8())
}

func (t *Tree) Height8() int8 {
	if t.root == nil {
		return 0
	}
	return t.root.height
}

// Has returns whether or not a key exists.
func (t *Tree) Has(key []byte) bool {
	if t.root == nil {
		return false
	}
	return t.root.has(t, key)
}

// Hash returns the root hash.
func (t *Tree) Hash() []byte {
	if t.root == nil {
		return nil
	}
	hash, _ := t.root.hashWithCount()
	return hash
}

// hashWithCount returns the root hash and hash count.
func (t *Tree) hashWithCount() ([]byte, int64) {
	if t.root == nil {
		return nil, 0
	}
	return t.root.hashWithCount()
}

// Get returns the index and value of the specified key if it exists, or nil
// and the next index, if it doesn't.
func (t *Tree) Get(key []byte) (index int, value []byte) {
	index64, value := t.Get64(key)
	return int(index64), value
}

func (t *Tree) Get64(key []byte) (index int64, value []byte) {
	if t.root == nil {
		return 0, nil
	}
	return t.root.get(t, key)
}

// GetByIndex gets the key and value at the specified index.
func (t *Tree) GetByIndex(index int) (key []byte, value []byte) {
	return t.GetByIndex64(int64(index))
}

func (t *Tree) GetByIndex64(index int64) (key []byte, value []byte) {
	if t.root == nil {
		return nil, nil
	}
	return t.root.getByIndex(t, index)
}

// Iterate iterates over all keys of the tree, in order.
func (t *Tree) Iterate(fn func(key []byte, value []byte) bool) (stopped bool) {
	if t.root == nil {
		return false
	}
	return t.root.traverse(t, true, func(node *Node) bool {
		if node.height == 0 {
			return fn(node.key, node.value)
		}
		return false
	})
}

// IterateRange makes a callback for all nodes with key between start and end non-inclusive.
// If either are nil, then it is open on that side (nil, nil is the same as Iterate)
func (t *Tree) IterateRange(start, end []byte, ascending bool, fn func(key []byte, value []byte) bool) (stopped bool) {
	if t.root == nil {
		return false
	}
	return t.root.traverseInRange(t, start, end, ascending, false, 0, func(node *Node, _ uint8) bool {
		if node.height == 0 {
			return fn(node.key, node.value)
		}
		return false
	})
}

// IterateRangeInclusive makes a callback for all nodes with key between start and end inclusive.
// If either are nil, then it is open on that side (nil, nil is the same as Iterate)
func (t *Tree) IterateRangeInclusive(start, end []byte, ascending bool, fn func(key, value []byte, version int64) bool) (stopped bool) {
	if t.root == nil {
		return false
	}
	return t.root.traverseInRange(t, start, end, ascending, true, 0, func(node *Node, _ uint8) bool {
		if node.height == 0 {
			return fn(node.key, node.value, node.version)
		}
		return false
	})
}

// Clone creates a clone of the tree.
// Used internally by VersionedTree.
func (t *Tree) clone() *Tree {
	return &Tree{
		root:    t.root,
		ndb:     t.ndb,
		version: t.version,
	}
}

// nodeSize is like Size, but includes inner nodes too.
func (t *Tree) nodeSize() int {
	size := 0
	t.root.traverse(t, true, func(n *Node) bool {
		size++
		return false
	})
	return size
}
