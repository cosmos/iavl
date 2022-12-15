package iavl

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"

	"github.com/cosmos/iavl/cache"

	"github.com/cosmos/iavl/internal/encoding"
)

// NodeKey represents a key of node in the DB.
type NodeKey struct {
	version int64
	path    *big.Int
}

func (nk *NodeKey) GetKey() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(nk.version))
	return append(b, nk.path.Bytes()...)
}

// Node represents a node in a Tree.
type Node struct {
	key           []byte
	value         []byte
	hash          []byte
	nodeKey       *NodeKey
	leftNodeKey   *NodeKey
	rightNodeKey  *NodeKey
	size          int64
	leftNode      *Node
	rightNode     *Node
	subtreeHeight int8
}

var _ cache.Node = (*Node)(nil)

// NewNode returns a new node from a key, value and version.
func NewNode(key []byte, value []byte) *Node {
	return &Node{
		key:           key,
		value:         value,
		subtreeHeight: 0,
		size:          1,
	}
}

// MakeNode constructs an *Node from an encoded byte slice.
//
// The new node doesn't have its hash saved or set. The caller must set it
// afterwards.
func MakeNode(nodeKey *NodeKey, buf []byte) (*Node, error) {
	// Read node header (height, size, key).
	height, n, cause := encoding.DecodeVarint(buf)
	if cause != nil {
		return nil, fmt.Errorf("decoding node.height, %w", cause)
	}
	buf = buf[n:]
	if height < int64(math.MinInt8) || height > int64(math.MaxInt8) {
		return nil, errors.New("invalid height, must be int8")
	}

	size, n, cause := encoding.DecodeVarint(buf)
	if cause != nil {
		return nil, fmt.Errorf("decoding node.size, %w", cause)
	}
	buf = buf[n:]

	key, n, cause := encoding.DecodeBytes(buf)
	if cause != nil {
		return nil, fmt.Errorf("decoding node.key, %w", cause)
	}
	buf = buf[n:]

	node := &Node{
		subtreeHeight: int8(height),
		size:          size,
		nodeKey:       nodeKey,
		key:           key,
	}

	// Read node body.
	if node.isLeaf() {
		val, _, cause := encoding.DecodeBytes(buf)
		if cause != nil {
			return nil, fmt.Errorf("decoding node.value, %w", cause)
		}
		node.value = val
		// ensure take the hash for the leaf node
		if _, err := node._hash(node.nodeKey.version); err != nil {
			return nil, fmt.Errorf("calculating hash error: %v", err)
		}

	} else { // Read children.
		node.hash, n, cause = encoding.DecodeBytes(buf)
		if cause != nil {
			return nil, fmt.Errorf("decoding node.hash, %w", cause)
		}
		buf = buf[n:]

		childType, n, cause := encoding.DecodeVarint(buf)
		if cause != nil {
			return nil, fmt.Errorf("deocding child type, %w", cause)
		}
		buf = buf[n:]
		if childType < int64(math.MinInt8) || childType > int64(math.MaxInt8) {
			return nil, errors.New("invalid child type, must be int8")
		}

		if childType > 0 {
			childVersion, n, cause := encoding.DecodeVarint(buf)
			if cause != nil {
				return nil, fmt.Errorf("decoding childNodeKey.version, %w", cause)
			}
			buf = buf[n:]

			childPath, n, cause := encoding.DecodeBytes(buf)
			if cause != nil {
				return nil, fmt.Errorf("decoding childNodeKey.path, %w", cause)
			}

			switch childType {
			case 1:
				node.leftNodeKey = &NodeKey{
					version: childVersion,
					path:    big.NewInt(0).SetBytes(childPath),
				}
			case 2:
				node.rightNodeKey = &NodeKey{
					version: childVersion,
					path:    big.NewInt(0).SetBytes(childPath),
				}
			}
		}
	}
	return node, nil
}

func (node *Node) GetKey() []byte {
	return node.nodeKey.GetKey()
}

// String returns a string representation of the node.
func (node *Node) String() string {
	child := ""
	if node.leftNode != nil && node.leftNode.nodeKey != nil {
		child += fmt.Sprintf("{left %d, %v}", node.leftNode.nodeKey.version, node.leftNode.nodeKey.path)
	}
	if node.rightNode != nil && node.rightNode.nodeKey != nil {
		child += fmt.Sprintf("{right %d, %v}", node.rightNode.nodeKey.version, node.rightNode.nodeKey.path)
	}
	return fmt.Sprintf("Node{%s:%s@ %v:%v-%v %d-%d}#%s",
		ColoredBytes(node.key, Green, Blue),
		ColoredBytes(node.value, Cyan, Blue),
		node.nodeKey, node.leftNodeKey, node.rightNodeKey,
		node.size, node.subtreeHeight, child)
}

// clone creates a shallow copy of a node with its hash set to nil.
func (node *Node) clone(tree *MutableTree) (*Node, error) {
	if node.isLeaf() {
		return nil, ErrCloneLeafNode
	}

	// ensure get children
	var err error
	leftNode := node.leftNode
	rightNode := node.rightNode
	if node.nodeKey != nil {
		leftNode, err = node.getLeftNode(tree.ImmutableTree)
		if err != nil {
			return nil, err
		}
		rightNode, err = node.getRightNode(tree.ImmutableTree)
		if err != nil {
			return nil, err
		}
		node.leftNode = nil
		node.rightNode = nil
	}

	return &Node{
		key:           node.key,
		subtreeHeight: node.subtreeHeight,
		size:          node.size,
		hash:          nil,
		nodeKey:       nil,
		leftNodeKey:   node.leftNodeKey,
		rightNodeKey:  node.rightNodeKey,
		leftNode:      leftNode,
		rightNode:     rightNode,
	}, nil
}

func (node *Node) isLeaf() bool {
	return node.subtreeHeight == 0
}

// Check if the node has a descendant with the given key.
func (node *Node) has(t *ImmutableTree, key []byte) (has bool, err error) {
	if bytes.Equal(node.key, key) {
		return true, nil
	}
	if node.isLeaf() {
		return false, nil
	}
	if bytes.Compare(key, node.key) < 0 {
		leftNode, err := node.getLeftNode(t)
		if err != nil {
			return false, err
		}
		return leftNode.has(t, key)
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return false, err
	}

	return rightNode.has(t, key)
}

// Get a key under the node.
//
// The index is the index in the list of leaf nodes sorted lexicographically by key. The leftmost leaf has index 0.
// It's neighbor has index 1 and so on.
func (node *Node) get(t *ImmutableTree, key []byte) (index int64, value []byte, err error) {
	if node.isLeaf() {
		switch bytes.Compare(node.key, key) {
		case -1:
			return 1, nil, nil
		case 1:
			return 0, nil, nil
		default:
			return 0, node.value, nil
		}
	}

	if bytes.Compare(key, node.key) < 0 {
		leftNode, err := node.getLeftNode(t)
		if err != nil {
			return 0, nil, err
		}

		return leftNode.get(t, key)
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return 0, nil, err
	}

	index, value, err = rightNode.get(t, key)
	if err != nil {
		return 0, nil, err
	}

	index += node.size - rightNode.size
	return index, value, nil
}

func (node *Node) getByIndex(t *ImmutableTree, index int64) (key []byte, value []byte, err error) {
	if node.isLeaf() {
		if index == 0 {
			return node.key, node.value, nil
		}
		return nil, nil, nil
	}
	// TODO: could improve this by storing the
	// sizes as well as left/right hash.
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return nil, nil, err
	}

	if index < leftNode.size {
		return leftNode.getByIndex(t, index)
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return nil, nil, err
	}

	return rightNode.getByIndex(t, index-leftNode.size)
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *Node) _hash(version int64) ([]byte, error) {
	if node.hash != nil {
		return node.hash, nil
	}

	h := sha256.New()
	buf := new(bytes.Buffer)
	if err := node.writeHashBytes(buf, version); err != nil {
		return nil, err
	}
	_, err := h.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	node.hash = h.Sum(nil)

	return node.hash, nil
}

// Hash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the node hash and number of nodes hashed.
// If the tree is empty (i.e. the node is nil), returns the hash of an empty input,
// to conform with RFC-6962.
func (node *Node) hashWithCount(version int64) ([]byte, error) {
	if node == nil {
		return sha256.New().Sum(nil), nil
	}
	if node.hash != nil {
		return node.hash, nil
	}

	h := sha256.New()
	buf := new(bytes.Buffer)
	err := node.writeHashBytesRecursively(buf, version)
	if err != nil {
		return nil, err
	}
	_, err = h.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	node.hash = h.Sum(nil)

	return node.hash, nil
}

// validate validates the node contents
func (node *Node) validate() error {
	if node == nil {
		return errors.New("node cannot be nil")
	}
	if node.key == nil {
		return errors.New("key cannot be nil")
	}
	if node.nodeKey == nil {
		return errors.New("nodeKey cannot be nil")
	}
	if node.nodeKey.version <= 0 {
		return errors.New("version must be greater than 0")
	}
	if node.subtreeHeight < 0 {
		return errors.New("height cannot be less than 0")
	}
	if node.size < 1 {
		return errors.New("size must be at least 1")
	}

	if node.subtreeHeight == 0 {
		// Leaf nodes
		if node.value == nil {
			return errors.New("value cannot be nil for leaf node")
		}
		if node.leftNodeKey != nil || node.leftNode != nil || node.rightNodeKey != nil || node.rightNode != nil {
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
		if node.leftNodeKey == nil || node.rightNodeKey == nil {
			return errors.New("inner node must have children")
		}
	}
	return nil
}

// Writes the node's hash to the given io.Writer. This function expects
// child hashes to be already set.
func (node *Node) writeHashBytes(w io.Writer, version int64) error {
	err := encoding.EncodeVarint(w, int64(node.subtreeHeight))
	if err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	err = encoding.EncodeVarint(w, node.size)
	if err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	err = encoding.EncodeVarint(w, version)
	if err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.isLeaf() {
		err = encoding.EncodeBytes(w, node.key)
		if err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

		err = encoding.EncodeBytes(w, valueHash[:])
		if err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		if node.leftNode == nil || node.rightNode == nil {
			return ErrEmptyChild
		}
		err = encoding.EncodeBytes(w, node.leftNode.hash)
		if err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		err = encoding.EncodeBytes(w, node.rightNode.hash)
		if err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

// Writes the node's hash to the given io.Writer.
// This function has the side-effect of calling hashWithCount.
func (node *Node) writeHashBytesRecursively(w io.Writer, version int64) error {
	_, err := node.leftNode.hashWithCount(version)
	if err != nil {
		return err
	}
	_, err = node.rightNode.hashWithCount(version)
	if err != nil {
		return err
	}
	return node.writeHashBytes(w, version)
}

func (node *Node) encodedSize() int {
	n := 1 +
		encoding.EncodeVarintSize(node.size) +
		encoding.EncodeBytesSize(node.key)
	if node.isLeaf() {
		n += encoding.EncodeBytesSize(node.value)
	} else {
		n += encoding.EncodeBytesSize(node.hash)
		if node.leftNodeKey != nil {
			n += encoding.EncodeVarintSize(node.leftNodeKey.version) +
				encoding.EncodeBytesSize(node.leftNodeKey.path.Bytes())
		}
		if node.rightNodeKey != nil {
			n += encoding.EncodeVarintSize(node.rightNodeKey.version) +
				encoding.EncodeBytesSize(node.rightNodeKey.path.Bytes())
		}
	}
	return n
}

// Writes the node as a serialized byte slice to the supplied io.Writer.
func (node *Node) writeBytes(w io.Writer) error {
	if node == nil {
		return errors.New("cannot write nil node")
	}
	cause := encoding.EncodeVarint(w, int64(node.subtreeHeight))
	if cause != nil {
		return fmt.Errorf("writing height, %w", cause)
	}
	cause = encoding.EncodeVarint(w, node.size)
	if cause != nil {
		return fmt.Errorf("writing size, %w", cause)
	}

	// Unlike writeHashByte, key is written for inner nodes.
	cause = encoding.EncodeBytes(w, node.key)
	if cause != nil {
		return fmt.Errorf("writing key, %w", cause)
	}

	if node.isLeaf() {
		cause = encoding.EncodeBytes(w, node.value)
		if cause != nil {
			return fmt.Errorf("writing value, %w", cause)
		}
	} else {
		cause = encoding.EncodeBytes(w, node.hash)
		if cause != nil {
			return fmt.Errorf("writing hash, %w", cause)
		}
		childType := int64(0)
		var childNodeKey NodeKey
		if node.leftNodeKey != nil {
			childType += 1
			childNodeKey = *node.leftNodeKey
		}
		if node.rightNodeKey != nil {
			childType += 2
			childNodeKey = *node.rightNodeKey
		}

		if childType > 2 {
			return ErrChildNodeKey
		}

		if cause := encoding.EncodeVarint(w, childType); cause != nil {
			return fmt.Errorf("writing childType , %w", cause)
		}

		if childType > 0 {
			if cause := encoding.EncodeVarint(w, childNodeKey.version); cause != nil {
				return fmt.Errorf("writing the version of the child node key, %w", cause)
			}
			if cause := encoding.EncodeBytes(w, childNodeKey.path.Bytes()); cause != nil {
				return fmt.Errorf("writing the path of the child node key, %w", cause)
			}
		}
	}
	return nil
}

func (node *Node) getLeftNode(t *ImmutableTree) (*Node, error) {
	if node.leftNode != nil {
		return node.leftNode, nil
	}
	leftNode, err := t.ndb.GetNode(node.leftNodeKey)
	if err != nil {
		return nil, err
	}
	return leftNode, nil
}

func (node *Node) getRightNode(t *ImmutableTree) (*Node, error) {
	if node.rightNode != nil {
		return node.rightNode, nil
	}
	rightNode, err := t.ndb.GetNode(node.rightNodeKey)
	if err != nil {
		return nil, err
	}
	return rightNode, nil
}

// NOTE: mutates height and size
func (node *Node) calcHeightAndSize(t *ImmutableTree) error {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return err
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return err
	}

	node.subtreeHeight = maxInt8(leftNode.subtreeHeight, rightNode.subtreeHeight) + 1
	node.size = leftNode.size + rightNode.size
	return nil
}

func (node *Node) calcBalance(t *ImmutableTree) (int, error) {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return 0, err
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return 0, err
	}

	return int(leftNode.subtreeHeight) - int(rightNode.subtreeHeight), nil
}

// traverse is a wrapper over traverseInRange when we want the whole tree
func (node *Node) traverse(t *ImmutableTree, ascending bool, cb func(*Node) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, false, func(node *Node) bool {
		return cb(node)
	})
}

// traversePost is a wrapper over traverseInRange when we want the whole tree post-order
func (node *Node) traversePost(t *ImmutableTree, ascending bool, cb func(*Node) bool) bool {
	return node.traverseInRange(t, nil, nil, ascending, false, true, func(node *Node) bool {
		return cb(node)
	})
}

func (node *Node) traverseInRange(tree *ImmutableTree, start, end []byte, ascending bool, inclusive bool, post bool, cb func(*Node) bool) bool {
	stop := false
	t := node.newTraversal(tree, start, end, ascending, inclusive, post)
	// TODO: figure out how to handle these errors
	for node2, err := t.next(); node2 != nil && err == nil; node2, err = t.next() {
		stop = cb(node2)
		if stop {
			return stop
		}
	}
	return stop
}

var (
	ErrCloneLeafNode  = fmt.Errorf("attempt to copy a leaf node")
	ErrEmptyChild     = fmt.Errorf("found an empty child")
	ErrChildNodeKey   = fmt.Errorf("node should have at most one child node key")
	ErrLeftHashIsNil  = fmt.Errorf("node.leftHash was nil in writeBytes")
	ErrRightHashIsNil = fmt.Errorf("node.rightHash was nil in writeBytes")
)
