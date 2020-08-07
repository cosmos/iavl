package iavl

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math"

	"github.com/pkg/errors"
)

// Node represents a node in a Tree.
type Node struct {
	key       []byte
	value     []byte
	hash      []byte
	leftHash  []byte
	rightHash []byte
	version   int64
	size      int64
	leftNode  *Node
	rightNode *Node
	height    int8
	persisted bool
}

// NewNode returns a new node from a key, value and version.
func NewNode(key []byte, value []byte, version int64) *Node {
	return &Node{
		key:     key,
		value:   value,
		height:  0,
		size:    1,
		version: version,
	}
}

// MakeNode constructs an *Node from an encoded byte slice.
//
// The new node doesn't have its hash saved or set. The caller must set it
// afterwards.
func MakeNode(buf []byte) (*Node, error) {

	// Read node header (height, size, version, key).
	height, n, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.height")
	}
	buf = buf[n:]
	if height < int64(math.MinInt8) || height > int64(math.MaxInt8) {
		return nil, errors.New("invalid height, must be int8")
	}

	size, n, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.size")
	}
	buf = buf[n:]

	ver, n, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.version")
	}
	buf = buf[n:]

	key, n, cause := decodeBytes(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding node.key")
	}
	buf = buf[n:]

	node := &Node{
		height:  int8(height),
		size:    size,
		version: ver,
		key:     key,
	}

	// Read node body.

	if node.isLeaf() {
		val, _, cause := decodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "decoding node.value")
		}
		node.value = val
	} else { // Read children.
		leftHash, n, cause := decodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "deocding node.leftHash")
		}
		buf = buf[n:]

		rightHash, _, cause := decodeBytes(buf)
		if cause != nil {
			return nil, errors.Wrap(cause, "decoding node.rightHash")
		}
		node.leftHash = leftHash
		node.rightHash = rightHash
	}
	return node, nil
}

// String returns a string representation of the node.
func (node *Node) String() string {
	hashstr := "<no hash>"
	if len(node.hash) > 0 {
		hashstr = fmt.Sprintf("%X", node.hash)
	}
	return fmt.Sprintf("Node{%s:%s@%d %X;%X}#%s",
		ColoredBytes(node.key, Green, Blue),
		ColoredBytes(node.value, Cyan, Blue),
		node.version,
		node.leftHash, node.rightHash,
		hashstr)
}

// clone creates a shallow copy of a node with its hash set to nil.
func (node *Node) clone(version int64) *Node {
	if node.isLeaf() {
		panic("Attempt to copy a leaf node")
	}
	return &Node{
		key:       node.key,
		height:    node.height,
		version:   version,
		size:      node.size,
		hash:      nil,
		leftHash:  node.leftHash,
		leftNode:  node.leftNode,
		rightHash: node.rightHash,
		rightNode: node.rightNode,
		persisted: false,
	}
}

func (node *Node) isLeaf() bool {
	return node.height == 0
}

// Check if the node has a descendant with the given key.
func (node *Node) has(t *ImmutableTree, key []byte) (has bool) {
	if bytes.Equal(node.key, key) {
		return true
	}
	if node.isLeaf() {
		return false
	}
	if bytes.Compare(key, node.key) < 0 {
		return node.getLeftNode(t).has(t, key)
	}
	return node.getRightNode(t).has(t, key)
}

// Get a key under the node.
func (node *Node) get(t *ImmutableTree, key []byte) (index int64, value []byte) {
	if node.isLeaf() {
		switch bytes.Compare(node.key, key) {
		case -1:
			return 1, nil
		case 1:
			return 0, nil
		default:
			return 0, node.value
		}
	}

	if bytes.Compare(key, node.key) < 0 {
		return node.getLeftNode(t).get(t, key)
	}
	rightNode := node.getRightNode(t)
	index, value = rightNode.get(t, key)
	index += node.size - rightNode.size
	return index, value
}

func (node *Node) getByIndex(t *ImmutableTree, index int64) (key []byte, value []byte) {
	if node.isLeaf() {
		if index == 0 {
			return node.key, node.value
		}
		return nil, nil
	}
	// TODO: could improve this by storing the
	// sizes as well as left/right hash.
	leftNode := node.getLeftNode(t)

	if index < leftNode.size {
		return leftNode.getByIndex(t, index)
	}
	return node.getRightNode(t).getByIndex(t, index-leftNode.size)
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *Node) _hash() []byte {
	if node.hash != nil {
		return node.hash
	}

	h := sha256.New()
	buf := new(bytes.Buffer)
	if err := node.writeHashBytes(buf); err != nil {
		panic(err)
	}
	_, err := h.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
	node.hash = h.Sum(nil)

	return node.hash
}

// Hash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the node hash and number of nodes hashed.
// If the tree is empty (i.e. the node is nil), returns the hash of an empty input,
// to conform with RFC-6962.
func (node *Node) hashWithCount() ([]byte, int64) {
	if node == nil {
		return sha256.New().Sum(nil), 0
	}
	if node.hash != nil {
		return node.hash, 0
	}

	h := sha256.New()
	buf := new(bytes.Buffer)
	hashCount, err := node.writeHashBytesRecursively(buf)
	if err != nil {
		panic(err)
	}
	_, err = h.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
	node.hash = h.Sum(nil)

	return node.hash, hashCount + 1
}

// validate validates the node contents
func (node *Node) validate() error {
	if node == nil {
		return errors.New("node cannot be nil")
	}
	if node.key == nil {
		return errors.New("key cannot be nil")
	}
	if node.version <= 0 {
		return errors.New("version must be greater than 0")
	}
	if node.height < 0 {
		return errors.New("height cannot be less than 0")
	}
	if node.size < 1 {
		return errors.New("size must be at least 1")
	}

	if node.height == 0 {
		// Leaf nodes
		if node.value == nil {
			return errors.New("value cannot be nil for leaf node")
		}
		if node.leftHash != nil || node.leftNode != nil || node.rightHash != nil || node.rightNode != nil {
			return errors.New("leaf node cannot have children")
		}
		if node.size != 1 {
			return errors.New("leaf nodes must have size 1")
		}
	} else {
		// Inner nodes
		if node.value != nil {
			return errors.New("value must be nil for non-leaf node")
		}
		if node.leftHash == nil && node.rightHash == nil {
			return errors.New("inner node must have children")
		}
	}
	return nil
}

// Writes the node's hash to the given io.Writer. This function expects
// child hashes to be already set.
func (node *Node) writeHashBytes(w io.Writer) error {
	err := encodeVarint(w, int64(node.height))
	if err != nil {
		return errors.Wrap(err, "writing height")
	}
	err = encodeVarint(w, node.size)
	if err != nil {
		return errors.Wrap(err, "writing size")
	}
	err = encodeVarint(w, node.version)
	if err != nil {
		return errors.Wrap(err, "writing version")
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.isLeaf() {
		err = encodeBytes(w, node.key)
		if err != nil {
			return errors.Wrap(err, "writing key")
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

		err = encodeBytes(w, valueHash[:])
		if err != nil {
			return errors.Wrap(err, "writing value")
		}
	} else {
		if node.leftHash == nil || node.rightHash == nil {
			panic("Found an empty child hash")
		}
		err = encodeBytes(w, node.leftHash)
		if err != nil {
			return errors.Wrap(err, "writing left hash")
		}
		err = encodeBytes(w, node.rightHash)
		if err != nil {
			return errors.Wrap(err, "writing right hash")
		}
	}

	return nil
}

// Writes the node's hash to the given io.Writer.
// This function has the side-effect of calling hashWithCount.
func (node *Node) writeHashBytesRecursively(w io.Writer) (hashCount int64, err error) {
	if node.leftNode != nil {
		leftHash, leftCount := node.leftNode.hashWithCount()
		node.leftHash = leftHash
		hashCount += leftCount
	}
	if node.rightNode != nil {
		rightHash, rightCount := node.rightNode.hashWithCount()
		node.rightHash = rightHash
		hashCount += rightCount
	}
	err = node.writeHashBytes(w)

	return
}

func (node *Node) encodedSize() int {
	n := 1 +
		encodeVarintSize(node.size) +
		encodeVarintSize(node.version) +
		encodeBytesSize(node.key)
	if node.isLeaf() {
		n += encodeBytesSize(node.value)
	} else {
		n += encodeBytesSize(node.leftHash) +
			encodeBytesSize(node.rightHash)
	}
	return n
}

// Writes the node as a serialized byte slice to the supplied io.Writer.
func (node *Node) writeBytes(w io.Writer) error {
	if node == nil {
		return errors.New("cannot write nil node")
	}
	cause := encodeVarint(w, int64(node.height))
	if cause != nil {
		return errors.Wrap(cause, "writing height")
	}
	cause = encodeVarint(w, node.size)
	if cause != nil {
		return errors.Wrap(cause, "writing size")
	}
	cause = encodeVarint(w, node.version)
	if cause != nil {
		return errors.Wrap(cause, "writing version")
	}

	// Unlike writeHashBytes, key is written for inner nodes.
	cause = encodeBytes(w, node.key)
	if cause != nil {
		return errors.Wrap(cause, "writing key")
	}

	if node.isLeaf() {
		cause = encodeBytes(w, node.value)
		if cause != nil {
			return errors.Wrap(cause, "writing value")
		}
	} else {
		if node.leftHash == nil {
			panic("node.leftHash was nil in writeBytes")
		}
		cause = encodeBytes(w, node.leftHash)
		if cause != nil {
			return errors.Wrap(cause, "writing left hash")
		}

		if node.rightHash == nil {
			panic("node.rightHash was nil in writeBytes")
		}
		cause = encodeBytes(w, node.rightHash)
		if cause != nil {
			return errors.Wrap(cause, "writing right hash")
		}
	}
	return nil
}

func (node *Node) getLeftNode(t *ImmutableTree) *Node {
	if node.leftNode != nil {
		return node.leftNode
	}
	return t.ndb.GetNode(node.leftHash)
}

func (node *Node) getRightNode(t *ImmutableTree) *Node {
	if node.rightNode != nil {
		return node.rightNode
	}
	return t.ndb.GetNode(node.rightHash)
}

// NOTE: mutates height and size
func (node *Node) calcHeightAndSize(t *ImmutableTree) {
	node.height = maxInt8(node.getLeftNode(t).height, node.getRightNode(t).height) + 1
	node.size = node.getLeftNode(t).size + node.getRightNode(t).size
}

func (node *Node) calcBalance(t *ImmutableTree) int {
	return int(node.getLeftNode(t).height) - int(node.getRightNode(t).height)
}

// traverse is a wrapper over traverseInRange when we want the whole tree
func (node *Node) traverse(t *ImmutableTree, ascending bool, cb func(*Node) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, 0, false, func(node *Node, depth uint8) bool {
		return cb(node)
	})
}

// traversePost is a wrapper over traverseInRange when we want the whole tree post-order
func (node *Node) traversePost(t *ImmutableTree, ascending bool, cb func(*Node) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, 0, true, func(node *Node, depth uint8) bool {
		return cb(node)
	})
}

func (node *Node) traverseInRange(t *ImmutableTree, start, end []byte, ascending bool, inclusive bool, depth uint8, post bool, cb func(*Node, uint8) bool) bool {
	if node == nil {
		return false
	}
	afterStart := start == nil || bytes.Compare(start, node.key) < 0
	startOrAfter := start == nil || bytes.Compare(start, node.key) <= 0
	beforeEnd := end == nil || bytes.Compare(node.key, end) < 0
	if inclusive {
		beforeEnd = end == nil || bytes.Compare(node.key, end) <= 0
	}

	// Run callback per inner/leaf node.
	stop := false
	if !post && (!node.isLeaf() || (startOrAfter && beforeEnd)) {
		stop = cb(node, depth)
		if stop {
			return stop
		}
	}

	if !node.isLeaf() {
		if ascending {
			// check lower nodes, then higher
			if afterStart {
				stop = node.getLeftNode(t).traverseInRange(t, start, end, ascending, inclusive, depth+1, post, cb)
			}
			if stop {
				return stop
			}
			if beforeEnd {
				stop = node.getRightNode(t).traverseInRange(t, start, end, ascending, inclusive, depth+1, post, cb)
			}
		} else {
			// check the higher nodes first
			if beforeEnd {
				stop = node.getRightNode(t).traverseInRange(t, start, end, ascending, inclusive, depth+1, post, cb)
			}
			if stop {
				return stop
			}
			if afterStart {
				stop = node.getLeftNode(t).traverseInRange(t, start, end, ascending, inclusive, depth+1, post, cb)
			}
		}
	}
	if stop {
		return stop
	}

	if post && (!node.isLeaf() || (startOrAfter && beforeEnd)) {
		stop = cb(node, depth)
		if stop {
			return stop
		}
	}

	return stop
}

// Only used in testing...
func (node *Node) lmd(t *ImmutableTree) *Node {
	if node.isLeaf() {
		return node
	}
	return node.getLeftNode(t).lmd(t)
}
