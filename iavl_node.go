package iavl

import (
	"bytes"
	"fmt"
	"io"

	"golang.org/x/crypto/ripemd160"

	"github.com/tendermint/go-wire"
	cmn "github.com/tendermint/tmlibs/common"
)

// IAVLNode represents a node in an IAVLTree.
type IAVLNode struct {
	key       []byte
	value     []byte
	version   uint64
	height    int8
	size      int
	hash      []byte
	leftHash  []byte
	leftNode  *IAVLNode
	rightHash []byte
	rightNode *IAVLNode
	persisted bool
}

// NewIAVLNode returns a new node from a key and value.
func NewIAVLNode(key []byte, value []byte) *IAVLNode {
	return &IAVLNode{
		key:     key,
		value:   value,
		height:  0,
		size:    1,
		version: 0,
	}
}

// MakeIAVLNode constructs an *IAVLNode from an encoded byte slice.
//
// The new node doesn't have its hash saved or set.  The caller must set it
// afterwards.
func MakeIAVLNode(buf []byte) (node *IAVLNode, err error) {
	node = &IAVLNode{}

	// Read node header.

	node.height = int8(buf[0])

	n := 1 // Keeps track of bytes read.
	buf = buf[n:]

	node.size, n, err = wire.GetVarint(buf)
	if err != nil {
		return nil, err
	}
	buf = buf[n:]

	node.key, n, err = wire.GetByteSlice(buf)
	if err != nil {
		return nil, err
	}
	buf = buf[n:]

	node.version = wire.GetUint64(buf)
	buf = buf[8:]

	// Read node body.

	if node.isLeaf() {
		node.value, n, err = wire.GetByteSlice(buf)
		if err != nil {
			return nil, err
		}
	} else { // Read children.
		leftHash, n, err := wire.GetByteSlice(buf)
		if err != nil {
			return nil, err
		}
		buf = buf[n:]

		rightHash, _, err := wire.GetByteSlice(buf)
		if err != nil {
			return nil, err
		}
		node.leftHash = leftHash
		node.rightHash = rightHash
	}
	return node, nil
}

// String returns a string representation of the node.
func (node *IAVLNode) String() string {
	if len(node.hash) == 0 {
		return "<no hash>"
	} else {
		return fmt.Sprintf("%x", node.hash)
	}
}

// debugString returns a string useful for printing a list of nodes.
func (node *IAVLNode) debugString() string {
	if node.value == nil && node.height > 0 {
		return fmt.Sprintf("%40x: %s   %-16s h=%d version=%d (left=%x, right=%x)", node.hash, node.key, "", node.height, node.version, node.leftHash, node.rightHash)
	} else {
		return fmt.Sprintf("%40x: %s = %-16s h=%d version=%d (left=%x, right=%x)", node.hash, node.key, node.value, node.height, node.version, node.leftHash, node.rightHash)
	}
}

// clone creates a shallow copy of a node with its hash set to nil.
func (node *IAVLNode) clone() *IAVLNode {
	return &IAVLNode{
		key:       node.key,
		value:     node.value,
		height:    node.height,
		version:   node.version,
		size:      node.size,
		hash:      nil,
		leftHash:  node.leftHash,
		leftNode:  node.leftNode,
		rightHash: node.rightHash,
		rightNode: node.rightNode,
		persisted: false,
	}
}

func (node *IAVLNode) isLeaf() bool {
	return node.height == 0
}

// Check if the node has a descendant with the given key.
func (node *IAVLNode) has(t *IAVLTree, key []byte) (has bool) {
	if bytes.Equal(node.key, key) {
		return true
	}
	if node.isLeaf() {
		return false
	}
	if bytes.Compare(key, node.key) < 0 {
		return node.getLeftNode(t).has(t, key)
	} else {
		return node.getRightNode(t).has(t, key)
	}
}

// Get a key under the node.
func (node *IAVLNode) get(t *IAVLTree, key []byte) (index int, value []byte, exists bool) {
	if node.isLeaf() {
		switch bytes.Compare(node.key, key) {
		case -1:
			return 1, nil, false
		case 1:
			return 0, nil, false
		default:
			return 0, node.value, true
		}
	}

	if bytes.Compare(key, node.key) < 0 {
		return node.getLeftNode(t).get(t, key)
	} else {
		rightNode := node.getRightNode(t)
		index, value, exists = rightNode.get(t, key)
		index += node.size - rightNode.size
		return index, value, exists
	}
}

func (node *IAVLNode) getByIndex(t *IAVLTree, index int) (key []byte, value []byte) {
	if node.isLeaf() {
		if index == 0 {
			return node.key, node.value
		} else {
			cmn.PanicSanity("getByIndex asked for invalid index")
			return nil, nil
		}
	} else {
		// TODO: could improve this by storing the
		// sizes as well as left/right hash.
		leftNode := node.getLeftNode(t)
		if index < leftNode.size {
			return leftNode.getByIndex(t, index)
		} else {
			return node.getRightNode(t).getByIndex(t, index-leftNode.size)
		}
	}
}

// Hash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the node hash and number of nodes hashed.
func (node *IAVLNode) hashWithCount() ([]byte, int) {
	if node.hash != nil {
		return node.hash, 0
	}

	hasher := ripemd160.New()
	buf := new(bytes.Buffer)
	_, hashCount, err := node.writeHashBytes(buf)
	if err != nil {
		cmn.PanicCrisis(err)
	}
	hasher.Write(buf.Bytes())
	node.hash = hasher.Sum(nil)

	return node.hash, hashCount + 1
}

// Writes the node's hash to the given io.Writer.
// This function has the side-effect of calling hashWithCount.
func (node *IAVLNode) writeHashBytes(w io.Writer) (n int, hashCount int, err error) {
	wire.WriteInt8(node.height, w, &n, &err)
	wire.WriteVarint(node.size, w, &n, &err)

	// Key is not written for inner nodes, unlike writeBytes.

	if node.isLeaf() {
		wire.WriteByteSlice(node.key, w, &n, &err)
		wire.WriteByteSlice(node.value, w, &n, &err)
		wire.WriteUint64(node.version, w, &n, &err)
	} else {
		if node.leftNode != nil {
			leftHash, leftCount := node.leftNode.hashWithCount()
			node.leftHash = leftHash
			hashCount += leftCount
		}
		if node.leftHash == nil {
			cmn.PanicSanity("node.leftHash was nil in writeHashBytes")
		}
		wire.WriteByteSlice(node.leftHash, w, &n, &err)

		if node.rightNode != nil {
			rightHash, rightCount := node.rightNode.hashWithCount()
			node.rightHash = rightHash
			hashCount += rightCount
		}
		if node.rightHash == nil {
			cmn.PanicSanity("node.rightHash was nil in writeHashBytes")
		}
		wire.WriteByteSlice(node.rightHash, w, &n, &err)
	}
	return
}

// Writes the node as a serialized byte slice to the supplied io.Writer.
func (node *IAVLNode) writeBytes(w io.Writer) (n int, err error) {
	wire.WriteInt8(node.height, w, &n, &err)
	wire.WriteVarint(node.size, w, &n, &err)

	// Unlike writeHashBytes, key is written for inner nodes.
	wire.WriteByteSlice(node.key, w, &n, &err)
	wire.WriteUint64(node.version, w, &n, &err)

	if node.isLeaf() {
		wire.WriteByteSlice(node.value, w, &n, &err)
	} else {
		if node.leftHash == nil {
			cmn.PanicSanity("node.leftHash was nil in writeBytes")
		}
		wire.WriteByteSlice(node.leftHash, w, &n, &err)

		if node.rightHash == nil {
			cmn.PanicSanity("node.rightHash was nil in writeBytes")
		}
		wire.WriteByteSlice(node.rightHash, w, &n, &err)
	}
	return
}

func (node *IAVLNode) set(t *IAVLTree, key []byte, value []byte) (
	newSelf *IAVLNode, updated bool, orphaned []*IAVLNode,
) {
	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case -1:
			return &IAVLNode{
				key:       node.key,
				height:    1,
				size:      2,
				leftNode:  NewIAVLNode(key, value),
				rightNode: node,
			}, false, []*IAVLNode{}
		case 1:
			return &IAVLNode{
				key:       key,
				height:    1,
				size:      2,
				leftNode:  node,
				rightNode: NewIAVLNode(key, value),
			}, false, []*IAVLNode{}
		default:
			return NewIAVLNode(key, value), true, []*IAVLNode{node}
		}
	} else {
		orphaned = append(orphaned, node)
		node = node.clone()

		if bytes.Compare(key, node.key) < 0 {
			var leftOrphaned []*IAVLNode
			node.leftNode, updated, leftOrphaned = node.getLeftNode(t).set(t, key, value)
			node.leftHash = nil // leftHash is yet unknown
			orphaned = append(orphaned, leftOrphaned...)
		} else {
			var rightOrphaned []*IAVLNode
			node.rightNode, updated, rightOrphaned = node.getRightNode(t).set(t, key, value)
			node.rightHash = nil // rightHash is yet unknown
			orphaned = append(orphaned, rightOrphaned...)
		}

		if updated {
			return node, updated, orphaned
		} else {
			node.calcHeightAndSize(t)
			new, balanceOrphaned := node.balance(t)
			return new, updated, append(orphaned, balanceOrphaned...)
		}
	}
}

// newHash/newNode: The new hash or node to replace node after remove.
// newKey: new leftmost leaf key for tree after successfully removing 'key' if changed.
// value: removed value.
func (node *IAVLNode) remove(t *IAVLTree, key []byte) (
	newHash []byte, newNode *IAVLNode, newKey []byte, value []byte, orphaned []*IAVLNode,
) {
	if node.isLeaf() {
		if bytes.Equal(key, node.key) {
			return nil, nil, nil, node.value, []*IAVLNode{node}
		}
		return node.hash, node, nil, nil, []*IAVLNode{}
	}

	if bytes.Compare(key, node.key) < 0 {
		var newLeftHash []byte
		var newLeftNode *IAVLNode

		newLeftHash, newLeftNode, newKey, value, orphaned =
			node.getLeftNode(t).remove(t, key)

		if len(orphaned) == 0 {
			return node.hash, node, nil, value, []*IAVLNode{}
		} else if newLeftHash == nil && newLeftNode == nil { // left node held value, was removed
			return node.rightHash, node.rightNode, node.key, value, orphaned
		}
		orphaned = append(orphaned, node)

		node = node.clone()
		node.leftHash, node.leftNode = newLeftHash, newLeftNode
		node.calcHeightAndSize(t)
		node, balanceOrphaned := node.balance(t)

		return node.hash, node, newKey, value, append(orphaned, balanceOrphaned...)
	} else {
		var newRightHash []byte
		var newRightNode *IAVLNode

		newRightHash, newRightNode, newKey, value, orphaned =
			node.getRightNode(t).remove(t, key)

		if len(orphaned) == 0 {
			return node.hash, node, nil, value, []*IAVLNode{}
		} else if newRightHash == nil && newRightNode == nil { // right node held value, was removed
			return node.leftHash, node.leftNode, nil, value, orphaned
		}
		orphaned = append(orphaned, node)

		node = node.clone()
		node.rightHash, node.rightNode = newRightHash, newRightNode
		if newKey != nil {
			node.key = newKey
		}
		node.calcHeightAndSize(t)
		node, balanceOrphaned := node.balance(t)

		return node.hash, node, nil, value, append(orphaned, balanceOrphaned...)
	}
}

func (node *IAVLNode) getLeftNode(t *IAVLTree) *IAVLNode {
	if node.leftNode != nil {
		return node.leftNode
	}
	return t.ndb.GetNode(node.leftHash)
}

func (node *IAVLNode) getRightNode(t *IAVLTree) *IAVLNode {
	if node.rightNode != nil {
		return node.rightNode
	}
	return t.ndb.GetNode(node.rightHash)
}

// NOTE: overwrites node
// TODO: optimize balance & rotate
func (node *IAVLNode) rotateRight(t *IAVLTree) (*IAVLNode, *IAVLNode) {
	node = node.clone()
	l := node.getLeftNode(t)
	_l := l.clone()

	_lrHash, _lrCached := _l.rightHash, _l.rightNode
	_l.rightHash, _l.rightNode = node.hash, node
	node.leftHash, node.leftNode = _lrHash, _lrCached

	node.calcHeightAndSize(t)
	_l.calcHeightAndSize(t)

	return _l, l
}

// NOTE: overwrites node
// TODO: optimize balance & rotate
func (node *IAVLNode) rotateLeft(t *IAVLTree) (*IAVLNode, *IAVLNode) {
	node = node.clone()
	r := node.getRightNode(t)
	_r := r.clone()

	_rlHash, _rlCached := _r.leftHash, _r.leftNode
	_r.leftHash, _r.leftNode = node.hash, node
	node.rightHash, node.rightNode = _rlHash, _rlCached

	node.calcHeightAndSize(t)
	_r.calcHeightAndSize(t)

	return _r, r
}

// NOTE: mutates height and size
func (node *IAVLNode) calcHeightAndSize(t *IAVLTree) {
	node.height = maxInt8(node.getLeftNode(t).height, node.getRightNode(t).height) + 1
	node.size = node.getLeftNode(t).size + node.getRightNode(t).size
}

func (node *IAVLNode) calcBalance(t *IAVLTree) int {
	return int(node.getLeftNode(t).height) - int(node.getRightNode(t).height)
}

// NOTE: assumes that node can be modified
// TODO: optimize balance & rotate
func (node *IAVLNode) balance(t *IAVLTree) (newSelf *IAVLNode, orphaned []*IAVLNode) {
	if node.persisted {
		panic("Unexpected balance() call on persisted node")
	}
	balance := node.calcBalance(t)

	if balance > 1 {
		if node.getLeftNode(t).calcBalance(t) >= 0 {
			// Left Left Case
			new, orphaned := node.rotateRight(t)
			return new, []*IAVLNode{orphaned}
		} else {
			// Left Right Case
			var leftOrphaned *IAVLNode

			left := node.getLeftNode(t)
			node.leftHash = nil
			node.leftNode, leftOrphaned = left.rotateLeft(t)
			new, rightOrphaned := node.rotateRight(t)

			return new, []*IAVLNode{left, leftOrphaned, rightOrphaned}
		}
	}
	if balance < -1 {
		if node.getRightNode(t).calcBalance(t) <= 0 {
			// Right Right Case
			new, orphaned := node.rotateLeft(t)
			return new, []*IAVLNode{orphaned}
		} else {
			// Right Left Case
			var rightOrphaned *IAVLNode

			right := node.getRightNode(t)
			node.rightHash = nil
			node.rightNode, rightOrphaned = right.rotateRight(t)
			new, leftOrphaned := node.rotateLeft(t)

			return new, []*IAVLNode{right, leftOrphaned, rightOrphaned}
		}
	}
	// Nothing changed
	return node, []*IAVLNode{}
}

// traverse is a wrapper over traverseInRange when we want the whole tree
func (node *IAVLNode) traverse(t *IAVLTree, ascending bool, cb func(*IAVLNode) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, cb)
}

func (node *IAVLNode) traverseInRange(t *IAVLTree, start, end []byte, ascending bool, inclusive bool, cb func(*IAVLNode) bool) bool {
	afterStart := start == nil || bytes.Compare(start, node.key) <= 0
	beforeEnd := end == nil || bytes.Compare(node.key, end) < 0
	if inclusive {
		beforeEnd = end == nil || bytes.Compare(node.key, end) <= 0
	}

	stop := false
	if afterStart && beforeEnd {
		// IterateRange ignores this if not leaf
		stop = cb(node)
	}
	if stop {
		return stop
	}
	if node.isLeaf() {
		return stop
	}

	if ascending {
		// check lower nodes, then higher
		if afterStart {
			stop = node.getLeftNode(t).traverseInRange(t, start, end, ascending, inclusive, cb)
		}
		if stop {
			return stop
		}
		if beforeEnd {
			stop = node.getRightNode(t).traverseInRange(t, start, end, ascending, inclusive, cb)
		}
	} else {
		// check the higher nodes first
		if beforeEnd {
			stop = node.getRightNode(t).traverseInRange(t, start, end, ascending, inclusive, cb)
		}
		if stop {
			return stop
		}
		if afterStart {
			stop = node.getLeftNode(t).traverseInRange(t, start, end, ascending, inclusive, cb)
		}
	}

	return stop
}

// Only used in testing...
func (node *IAVLNode) lmd(t *IAVLTree) *IAVLNode {
	if node.isLeaf() {
		return node
	}
	return node.getLeftNode(t).lmd(t)
}

// Only used in testing...
func (node *IAVLNode) rmd(t *IAVLTree) *IAVLNode {
	if node.isLeaf() {
		return node
	}
	return node.getRightNode(t).rmd(t)
}
