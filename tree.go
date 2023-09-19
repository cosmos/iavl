package iavl

import (
	"bytes"
	"fmt"
	"os"
	"sync"
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
	pool           *sync.Pool
	checkpointer   *checkpointer

	workingBytes uint64
	workingSize  int64

	// options
	maxWorkingSize uint64
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.version++

	var sequence uint32
	tree.deepHash(&sequence, tree.root)

	if tree.workingBytes > tree.maxWorkingSize {
		if err := tree.asyncCheckpoint(); err != nil {
			return nil, tree.version, err
		}

		tree.lastCheckpoint = tree.version
		tree.orphans = nil
		tree.workingBytes = 0
		tree.workingSize = 0
		tree.cache.Swap()

		tree.root.leftNode = nil
		tree.root.rightNode = nil

	}

	return tree.root.hash, tree.version, nil
}

func (tree *Tree) asyncCheckpoint() error {
	tree.checkpointer.ch <- &checkpointArgs{
		set:     tree.buildCheckpoint(tree.root),
		delete:  tree.orphans,
		version: tree.version,
	}
	return nil
}

func (tree *Tree) checkpoint() error {
	start := time.Now()
	stats := &saveStats{}

	//log.Info().Msgf("dirty_count=%s", humanize.Comma(tree.dirtyCount(tree.root)))

	setCount, err := tree.deepSave(tree.root, stats)
	if err != nil {
		return err
	}

	//sets := tree.buildCheckpoint(tree.root)
	//if len(sets) != int(setCount) {
	//	return fmt.Errorf("set count mismatch; expected=%d, actual=%d", len(sets), setCount)
	//}

	for _, nk := range tree.orphans {
		if err := tree.db.Delete(nk); err != nil {
			return err
		}
	}

	log.Info().Msgf("checkpoint; version=%s, set=%s, del=%s, ws_size=%s, ws_bz=%s, save_bz=%s, db_bz=%s, dur=%s",
		humanize.Comma(tree.version),
		humanize.Comma(setCount),
		humanize.Comma(int64(len(tree.orphans))),
		humanize.Comma(tree.workingSize),
		humanize.IBytes(tree.workingBytes),
		humanize.IBytes(stats.nodeBz),
		humanize.IBytes(stats.dbBz),
		time.Since(start).Round(time.Millisecond),
	)

	return nil
}

type saveStats struct {
	nodeBz uint64
	dbBz   uint64
	count  int64
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

func (tree *Tree) dirtyCount(node *Node) int64 {
	var n int64
	if node.dirty {
		n = 1
	}

	if !node.isLeaf() {
		return n + tree.dirtyCount(node.left(tree)) + tree.dirtyCount(node.right(tree))
	} else {
		return n
	}
}

func (tree *Tree) buildCheckpoint(node *Node) (sets []*Node) {
	if node.nodeKey.version <= tree.lastCheckpoint {
		return nil
	}

	tree.cache.Set(node)
	node.dirty = false

	n := *node

	if node.isLeaf() {
		return []*Node{&n}
	} else {
		sets = append(sets, &n)
		sets = append(sets, tree.buildCheckpoint(node.left(tree))...)
		sets = append(sets, tree.buildCheckpoint(node.right(tree))...)

		node.leftNode = nil
		node.rightNode = nil

		return sets
	}
}

func (tree *Tree) deepSave(node *Node, stats *saveStats) (count int64, err error) {
	if node.nodeKey.version <= tree.lastCheckpoint {
		return 0, nil
	}

	if n, err := tree.db.Set(node); err != nil {
		return count, err
	} else {
		stats.dbBz += uint64(n)
	}
	stats.nodeBz += node.sizeBytes()

	node.dirty = false
	tree.cache.Set(node)

	if !node.isLeaf() && node.leftNode != nil {
		leftCount, err := tree.deepSave(node.leftNode, stats)
		if err != nil {
			return count, err
		}
		rightCount, err := tree.deepSave(node.rightNode, stats)
		if err != nil {
			return count, err
		}

		// clear children to prevent memory leaks here. after each checkpoint the tree is purged.
		node.leftNode = nil
		node.rightNode = nil

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
	//if tree.version == 6 && tree.workingSize == 2861 && node.isLeaf() {
	//	fmt.Println("here")
	//}
	if node == nil {
		panic("node is nil")
	}
	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1: // setKey < leafKey
			tree.metrics.PoolGet += 2
			n := tree.poolGet()
			n.key = node.key
			n.subtreeHeight = 1
			n.size = 2
			n.leftNode = tree.NewNode(key, value, tree.version)
			n.rightNode = node
			n.dirty = true

			tree.workingBytes += n.sizeBytes()
			tree.workingSize++
			return n, false, nil
		case 1: // setKey > leafKey
			tree.metrics.PoolGet += 2
			n := tree.poolGet()
			n.key = key
			n.subtreeHeight = 1
			n.size = 2
			n.leftNode = node
			n.rightNode = tree.NewNode(key, value, tree.version)
			n.dirty = true

			tree.workingBytes += n.sizeBytes()
			tree.workingSize++
			return n, false, nil
		default:
			tree.addOrphan(node)
			//return tree.NewNode(key, value, tree.version), true, nil

			//tree.mutateNode(node)
			//node.value = value
			//node._hash(tree.version + 1)
			//node.value = nil
			//return node, true, nil

			n := tree.poolGet()
			n.key = key
			n.value = value
			n.subtreeHeight = 0
			n.size = 1
			n._hash(tree.version + 1)
			n.value = nil
			n.dirty = true

			if !node.dirty {
				tree.workingBytes += n.sizeBytes()
				tree.workingSize++
			}
			return n, true, nil
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
			tree.returnNode(node)
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
			tree.returnNode(node)
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
		tree.returnNode(node)
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
	if node.dirty {
		return
	}

	node.dirty = true
	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
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

func (tree *Tree) poolGet() *Node {
	tree.metrics.PoolGet++
	node := tree.pool.Get().(*Node)
	node.leftNodeKey = nil
	node.rightNodeKey = nil
	node.rightNode = nil
	node.leftNode = nil
	node.nodeKey = nil
	node.hash = nil
	node.key = nil
	node.value = nil
	node.subtreeHeight = 0
	node.size = 0
	node.dirty = false
	return node
}

// NewNode returns a new node from a key, value and version.
func (tree *Tree) NewNode(key []byte, value []byte, version int64) *Node {
	//node := &Node{
	//	key:           key,
	//	value:         value,
	//	subtreeHeight: 0,
	//	size:          1,
	//}
	node := tree.poolGet()
	node.key = key
	node.value = value
	node.subtreeHeight = 0
	node.size = 1

	node._hash(version + 1)
	node.value = nil
	node.dirty = true
	tree.workingBytes += node.sizeBytes()
	tree.workingSize++
	return node
}

func (tree *Tree) returnNode(node *Node) {
	if node.dirty {
		tree.workingBytes -= node.sizeBytes()
		tree.workingSize--
	}
	tree.pool.Put(node)
}
