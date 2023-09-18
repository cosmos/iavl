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
	version int64
	root    *Node
	rootKey NodeKey
	pool    *nodePool
	metrics *metrics.TreeMetrics
	db      nodeDB

	// should be part of pool?
	orphans            []NodeKey
	checkpointInterval int64
	lastCheckpoint     int64

	checkpointing      bool
	checkpointSetCount int64
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.version++
	var sequence uint32

	if tree.shouldCheckpoint() {
		//if err := tree.Checkpoint(); err != nil {
		//	return nil, 0, err
		//}
		tree.pool.workingSize = 0
		tree.checkpointing = true
		log.Info().Msgf("checkpoint version %d", tree.version)
	}

	// deepHash flushes to disk and clears overflowed nodes for GC
	tree.rootKey = tree.deepHash(&sequence, tree.root)

	if tree.checkpointing {
		setCount, err := tree.deepSave(tree.root)
		if err != nil {
			return nil, tree.version, err
		}

		deleteMap := make(map[NodeKey]bool)
		for _, o := range tree.orphans {
			deleteMap[o] = true
			if err := tree.db.Delete(o); err != nil {
				return nil, tree.version, err
			}
		}

		log.Info().Msgf("checkpointed version %d; sets %s, deletes %s, real deletes: %s",
			tree.version, humanize.Comma(setCount),
			humanize.Comma(int64(len(tree.orphans))),
			humanize.Comma(int64(len(deleteMap))),
		)

		tree.orphans = nil
		tree.checkpointing = false
		tree.checkpointSetCount = 0
		tree.lastCheckpoint = tree.version
		// free all nodes for GC
		tree.root.leftNode = nil
		tree.root.rightNode = nil
	}

	return tree.root.hash, tree.version, nil
}

func (tree *Tree) Checkpoint() error {
	log.Info().Msgf("requesting checkpoint at version %d", tree.version)
	var sets []*Node
	for _, poolNode := range tree.pool.nodes {
		if poolNode.dirty && poolNode.NodeKey.Version() > tree.lastCheckpoint {
			// copy entire working set for checkpoint. this allows mutate replace dirty nodes. if we passed by
			// reference then mutate would replace dirty nodes in the working set and the checkpoint would be
			// invalid.
			// Alternative: mutate refuses to replace dirty in working set being checkpointed and instead fetches
			// a new node from the pool.
			// Hypothesis: this would result in the same amount of memory allocation but is operationally more complex.
			n := *poolNode
			n.leftNode = nil
			n.rightNode = nil
			n.Value = nil

			sets = append(sets, &n)
			//poolNode.dirty = false
			//tree.pool.dirtyCount--
			tree.pool.workingSize = 0
		}
	}

	tree.pool.checkpointCh <- &checkpointArgs{
		set:     sets,
		delete:  tree.orphans,
		version: tree.version,
	}

	tree.orphans = nil
	tree.lastCheckpoint = tree.version

	return nil
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

// Get returns the value of the specified key if it exists, or nil otherwise.
// The returned value must not be modified, since it may point to data stored within IAVL.
func (tree *Tree) Get(key []byte) ([]byte, error) {
	if tree.root == nil {
		return nil, nil
	}

	return tree.Get(key)
}

// Remove removes a key from the working tree. The given key byte slice should not be modified
// after this call, since it may point to data stored inside IAVL.
func (tree *Tree) Remove(key []byte) ([]byte, bool, error) {
	if tree.root == nil {
		return nil, false, nil
	}
	tree.root.use = true
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

func (tree *Tree) Size() int64 {
	return tree.root.Size
}

func (tree *Tree) Height() int8 {
	return tree.root.SubtreeHeight
}

func (tree *Tree) shouldCheckpoint() bool {
	if tree.pool.workingSize > tree.pool.maxWorkingSize {
		log.Info().Msgf("working set size %s > max working set size %s; dirtySize=%s poolSize=%s; time evicting=%s",
			humanize.IBytes(tree.pool.workingSize),
			humanize.IBytes(tree.pool.maxWorkingSize),
			humanize.IBytes(tree.pool.dirtySize),
			humanize.IBytes(tree.pool.poolSize),
			tree.pool.timeEvicting.Round(time.Second))
		return true
	}
	if tree.version-tree.lastCheckpoint > tree.checkpointInterval {
		return true
	}
	return false
}

// removes the node corresponding to the passed key and balances the tree.
// It returns:
// - the hash of the new node (or nil if the node is the one removed)
// - the node that replaces the orig. node after remove
// - new leftmost leaf key for tree after successfully removing 'key' if changed.
// - the removed value
func (tree *Tree) recursiveRemove(node *Node, key []byte) (newSelf *Node, newKey []byte, newValue []byte, removed bool, err error) {
	if node.isLeaf() {
		if bytes.Equal(key, node.Key) {
			tree.addOrphan(node)
			tree.pool.Return(node)
			return nil, nil, node.Value, true, nil
		}
		return node, nil, nil, false, nil
	}

	if err != nil {
		return nil, nil, nil, false, err
	}

	// node.key < key; we go to the left to find the key:
	if bytes.Compare(key, node.Key) < 0 {
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
			k := node.Key
			tree.pool.Return(node)
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
		tree.pool.Return(node)
		return left, nil, value, removed, nil
	}

	tree.mutateNode(node)

	node.setRight(newRightNode)
	if newKey != nil {
		node.Key = newKey
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

func (tree *Tree) set(key []byte, value []byte) (updated bool, err error) {
	if value == nil {
		return updated, fmt.Errorf("attempt to store nil value at key '%s'", key)
	}

	if tree.root == nil {
		tree.root = tree.pool.Get(key, value, tree.version)
		tree.root.Size = 1
		return updated, nil
	}

	// todo this is a hack to prevent the root node from being garbage collected
	// could be fixed by checking rootKey against the root node's key, or pinning root in
	// the pool
	tree.root.use = true

	tree.root, updated, err = tree.recursiveSet(tree.root, key, value)
	return updated, err
}

func (tree *Tree) recursiveSet(node *Node, key []byte, value []byte) (
	newSelf *Node, updated bool, err error,
) {
	if node.isLeaf() {
		switch bytes.Compare(key, node.Key) {
		case -1: // setKey < leafKey
			n := tree.pool.Get(node.Key, nil, tree.version)
			n.SubtreeHeight = 1
			n.Size = 2
			n.setRight(node)

			n.leftNode = tree.pool.Get(key, value, tree.version)
			n.leftNode.Size = 1
			return n, false, nil
		case 1: // setKey > leafKey
			n := tree.pool.Get(key, nil, tree.version)
			n.Key = key
			n.SubtreeHeight = 1
			n.Size = 2
			n.setLeft(node)

			n.rightNode = tree.pool.Get(key, value, tree.version)
			n.rightNode.Size = 1
			return n, false, nil
		default:
			tree.addOrphan(node)
			node.NodeKey = NodeKey{}
			node.Value = value
			node._hash(tree.version)
			node.Value = nil
			tree.pool.dirtyNode(node)
			return node, true, nil
		}
	} else {
		tree.addOrphan(node)
		tree.mutateNode(node)

		var newChild *Node
		if bytes.Compare(key, node.Key) < 0 {
			newChild, updated, err = tree.recursiveSet(node.left(tree), key, value)
			if err != nil {
				return nil, updated, err
			}
			node.setLeft(newChild)
		} else {
			newChild, updated, err = tree.recursiveSet(node.right(tree), key, value)
			if err != nil {
				return nil, updated, err
			}
			node.setRight(newChild)
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

func (tree *Tree) deepHash(sequence *uint32, node *Node) NodeKey {
	if node == nil {
		panic("nil node in deepHash")
	}

	if !node.NodeKey.IsEmpty() {
		return node.NodeKey
	}

	*sequence++
	node.NodeKey = NewNodeKey(tree.version, *sequence)
	if !node.isLeaf() {
		node.LeftNodeKey = tree.deepHash(sequence, node.left(tree))
		node.RightNodeKey = tree.deepHash(sequence, node.right(tree))
		node._hash(tree.version)
	}

	return node.NodeKey
}

func (tree *Tree) deepSave(node *Node) (count int64, err error) {
	if node.NodeKey.Version() <= tree.lastCheckpoint {
		return 0, nil
	}

	if err := tree.db.Set(node); err != nil {
		return count, err
	}

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

func (tree *Tree) addOrphan(n *Node) {
	//orphans which never made it to the db don't need to be deleted from it.
	if !n.NodeKey.IsEmpty() && n.NodeKey.Version() <= tree.lastCheckpoint {
		tree.orphans = append(tree.orphans, n.NodeKey)
	}
}

func (tree *Tree) mutateNode(node *Node) {
	if node.hash == nil {
		return
	}
	node.hash = nil
	node.NodeKey = NodeKey{}
	tree.pool.workingSize += node.sizeBytes()
	//tree.pool.dirtyNode(node)
}
