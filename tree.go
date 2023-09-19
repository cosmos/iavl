package iavl

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

var log = zlog.Output(zerolog.ConsoleWriter{
	Out:        os.Stderr,
	TimeFormat: time.Stamp,
})

type Tree struct {
	version        int64
	root           *Node
	metrics        *metrics.TreeMetrics
	db             *kvDB
	lastCheckpoint int64
	orphans        [][]byte
	cache          *NodeCache

	workingSetSize uint64
	maxWorkingSize uint64
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.version++

	var sequence uint32
	tree.deepHash(&sequence, tree.root)

	if tree.workingSetSize > tree.maxWorkingSize {
		log.Info().Msgf("working set size %s > max %s; checkpointing",
			humanize.IBytes(tree.workingSetSize),
			humanize.IBytes(tree.maxWorkingSize),
		)
		if err := tree.checkpoint(); err != nil {
			return nil, tree.version, err
		}
	}

	return tree.root.hash, tree.version, nil
}

func (tree *Tree) checkpoint() error {
	start := time.Now()

	setCount, err := tree.deepSave(tree.root)
	if err != nil {
		return err
	}
	for _, nk := range tree.orphans {
		if err := tree.db.Delete(nk); err != nil {
			return err
		}
	}

	log.Info().Msgf("checkpoint; version=%s, sets=%s, deletes=%s, dur=%s",
		humanize.Comma(tree.version),
		humanize.Comma(setCount),
		humanize.Comma(int64(len(tree.orphans))),
		time.Since(start).Round(time.Millisecond),
	)

	tree.lastCheckpoint = tree.version
	tree.orphans = nil
	tree.workingSetSize = 0
	tree.cache.Swap()

	tree.root.leftNode = nil
	tree.root.rightNode = nil

	return nil
}

func (tree *Tree) deepHash(sequence *uint32, node *Node) *NodeKey {
	if node.nodeKey != nil {
		return node.nodeKey
	}

	*sequence++
	node.nodeKey = &NodeKey{
		version:  tree.version,
		sequence: *sequence,
	}
	if !node.isLeaf() {
		// wrong, should be nodekey assignment, but just profiling for now.
		node.leftNodeKey = tree.deepHash(sequence, node.left(tree)).GetKey()
		node.rightNodeKey = tree.deepHash(sequence, node.right(tree)).GetKey()
	}

	node._hash(tree.version)

	return node.nodeKey
}

func (tree *Tree) deepSave(node *Node) (count int64, err error) {
	if node.nodeKey.version <= tree.lastCheckpoint {
		return 0, nil
	}

	if err := tree.db.Set(node); err != nil {
		return count, err
	}
	tree.cache.Set(node)

	if !node.isLeaf() {
		leftCount, err := tree.deepSave(node.left(tree))
		if err != nil {
			return count, err
		}
		rightCount, err := tree.deepSave(node.right(tree))
		if err != nil {
			return count, err
		}
		return leftCount + rightCount + 1, nil
	} else {
		return 1, nil
	}
}

// Set sets a key in the working tree. Nil values are invalid. The given
// key/value byte slices must not be modified after this call, since they point
// to slices stored within IAVL. It returns true when an existing value was
// updated, while false means it was a new key.
func (tree *Tree) Set(key, value []byte) (updated bool, err error) {
	updated, err = tree.set(key, value)
	if err != nil {
		return false, err
	}
	if updated {
		tree.metrics.TreeUpdate++
	} else {
		tree.metrics.TreeNewNode++
	}
	return updated, nil
}

func (tree *Tree) set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.root == nil {
		tree.root = tree.NewNode(key, value, tree.version)
		return updated, nil
	}

	tree.root, updated, err = tree.recursiveSet(tree.root, key, value)
	return updated, err
}

func (tree *Tree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	if node == nil {
		panic("node is nil")
	}
	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1: // setKey < leafKey
			tree.metrics.PoolGet += 2
			n := &Node{
				key:           node.key,
				subtreeHeight: 1,
				size:          2,
				nodeKey:       nil,
				leftNode:      tree.NewNode(key, value, tree.version),
				rightNode:     node,
			}
			tree.workingSetSize += n.sizeBytes()
			return n, false, nil
		case 1: // setKey > leafKey
			tree.metrics.PoolGet += 2
			n := &Node{
				key:           key,
				subtreeHeight: 1,
				size:          2,
				nodeKey:       nil,
				leftNode:      node,
				rightNode:     tree.NewNode(key, value, tree.version),
			}
			tree.workingSetSize += n.sizeBytes()
			return n, false, nil
		default:
			tree.addOrphan(node)
			return tree.NewNode(key, value, tree.version), true, nil
		}
	} else {
		tree.addOrphan(node)
		tree.mutateNode(node)

		if bytes.Compare(key, node.key) < 0 {
			node.leftNode, updated, err = tree.recursiveSet(node.left(tree), key, value)
			if err != nil {
				return nil, updated, err
			}
		} else {
			node.rightNode, updated, err = tree.recursiveSet(node.right(tree), key, value)
			if err != nil {
				return nil, updated, err
			}
		}

		if updated {
			return node, updated, nil
		}
		err = node.calcHeightAndSize(tree)
		if err != nil {
			return nil, false, err
		}
		newNode, err := tree.balance(node)
		if err != nil {
			return nil, false, err
		}
		return newNode, updated, err
	}
}

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.root == nil {
		return nil, false, nil
	}
	newRoot, _, value, removed, err := tree.recursiveRemove(tree.root, key)
	if err != nil {
		return nil, false, err
	}
	if !removed {
		return nil, false, nil
	}

	tree.metrics.TreeDelete++

	tree.root = newRoot
	return value, true, nil
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
func (tree *Tree) recursiveRemove(node *Node, key []byte) (newSelf *Node, newKey []byte, newValue []byte, removed bool, err error) {
	if node.isLeaf() {
		if bytes.Equal(key, node.key) {
			tree.addOrphan(node)
			//tree.pool.Return(node)
			return nil, nil, node.value, true, nil
		}
		return node, nil, nil, false, nil
	}

	if err != nil {
		return nil, nil, nil, false, err
	}

	// node.key < key; we go to the left to find the key:
	if bytes.Compare(key, node.key) < 0 {
		newLeftNode, newKey, value, removed, err := tree.recursiveRemove(node.left(tree), key)
		if err != nil {
			return nil, nil, nil, false, err
		}

		if !removed {
			return node, nil, value, removed, nil
		}

		tree.addOrphan(node)

		// left node held value, was removed
		// collapse `node.rightNode` into `node`
		if newLeftNode == nil {
			right := node.right(tree)
			k := node.key
			//tree.pool.Return(node)
			return right, k, value, removed, nil
		}

		tree.mutateNode(node)

		node.setLeft(newLeftNode)
		err = node.calcHeightAndSize(tree)
		if err != nil {
			return nil, nil, nil, false, err
		}
		node, err = tree.balance(node)
		if err != nil {
			return nil, nil, nil, false, err
		}

		return node, newKey, value, removed, nil
	}
	// node.key >= key; either found or look to the right:
	newRightNode, newKey, value, removed, err := tree.recursiveRemove(node.right(tree), key)
	if err != nil {
		return nil, nil, nil, false, err
	}

	if !removed {
		return node, nil, value, removed, nil
	}

	tree.addOrphan(node)

	// right node held value, was removed
	// collapse `node.leftNode` into `node`
	if newRightNode == nil {
		left := node.left(tree)
		//tree.pool.Return(node)
		return left, nil, value, removed, nil
	}

	tree.mutateNode(node)

	node.setRight(newRightNode)
	if newKey != nil {
		node.key = newKey
	}
	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, nil, nil, false, err
	}

	node, err = tree.balance(node)
	if err != nil {
		return nil, nil, nil, false, err
	}

	return node, nil, value, removed, nil
}

func (tree *Tree) Size() int64 {
	return tree.root.size
}

func (tree *Tree) Height() int8 {
	return tree.root.subtreeHeight
}

func (tree *Tree) mutateNode(node *Node) {
	// node has already been mutated in working set
	if node.nodeKey == nil {
		return
	}
	node.hash = nil
	node.nodeKey = nil
	tree.workingSetSize += node.sizeBytes()
}

func (tree *Tree) addOrphan(node *Node) {
	if node.nodeKey == nil {
		return
	}
	if node.nodeKey.version > tree.lastCheckpoint {
		return
	}
	tree.orphans = append(tree.orphans, node.nodeKey.GetKey())
}

// NewNode returns a new node from a key, value and version.
func (tree *Tree) NewNode(key []byte, value []byte, version int64) *Node {
	node := &Node{
		key:           key,
		value:         value,
		subtreeHeight: 0,
		size:          1,
	}
	node._hash(version + 1)
	node.value = nil
	tree.workingSetSize += node.sizeBytes()
	return node
}
