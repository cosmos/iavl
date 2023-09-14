package iavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unsafe"

	encoding "github.com/cosmos/iavl/v2/internal"
)

const nodeKeySize = 12

type NodeKey [nodeKeySize]byte

var emptyNodeKey = NodeKey{}

func NewNodeKey(version int64, sequence uint32) NodeKey {
	nk := NodeKey{}
	binary.BigEndian.PutUint64(nk[:], uint64(version))
	binary.BigEndian.PutUint32(nk[8:], sequence)
	return nk
}

func (nk NodeKey) Version() int64 {
	return int64(binary.BigEndian.Uint64(nk[:]))
}

func (nk NodeKey) Sequence() uint32 {
	return binary.BigEndian.Uint32(nk[8:])
}

func (nk NodeKey) IsEmpty() bool {
	return nk == emptyNodeKey
}

// String returns a string representation of the node key.
func (nk NodeKey) String() string {
	return fmt.Sprintf("(%d, %d)", nk.Version(), nk.Sequence())
}

// Node represents a node in a Tree.
type Node struct {
	Key           []byte
	Value         []byte
	NodeKey       NodeKey
	LeftNodeKey   NodeKey
	RightNodeKey  NodeKey
	Size          int64
	SubtreeHeight int8

	leftNode  *Node
	rightNode *Node
	hash      []byte

	frameId int
	use     bool
	dirty   bool
}

var nodeSize = int64(unsafe.Sizeof(Node{})) + hashSize

func (node *Node) varSize() int64 {
	return int64(len(node.Key)) + int64(len(node.Value))
}

// getLeftNode will never be called on leaf nodes. all tree nodes have 2 children.
func (node *Node) getLeftNode(t *Tree) (*Node, error) {
	if node.isLeaf() {
		return nil, fmt.Errorf("leaf node has no left node")
	}
	if node.leftNode == nil || node.LeftNodeKey != node.leftNode.NodeKey {
		if node.LeftNodeKey.IsEmpty() {
			return nil, fmt.Errorf("left node key is nil")
		}
		leftNode, err := t.db.Get(node.LeftNodeKey)
		if err != nil {
			return nil, err
		}
		if leftNode == nil {
			return nil, fmt.Errorf("left node is nil; fetch failed")
		}

		node.leftNode = leftNode
		t.pool.Put(node.leftNode)
	}
	node.leftNode.use = true
	return node.leftNode, nil
}

func (node *Node) left(t *Tree) *Node {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		panic(err)
	}
	return leftNode
}

func (node *Node) setLeft(leftNode *Node) {
	node.LeftNodeKey = leftNode.NodeKey
	node.leftNode = leftNode
}

func (node *Node) getRightNode(t *Tree) (*Node, error) {
	if node.isLeaf() {
		return nil, fmt.Errorf("leaf node has no right node")
	}
	if node.rightNode == nil || node.RightNodeKey != node.rightNode.NodeKey {
		if node.RightNodeKey.IsEmpty() {
			return nil, fmt.Errorf("right node key is nil")
		}

		rightNode, err := t.db.Get(node.RightNodeKey)
		if err != nil {
			return nil, err
		}
		if rightNode == nil {
			return nil, fmt.Errorf("right node is nil; fetch failed")
		}
		node.rightNode = rightNode
		t.pool.Put(node.rightNode)
	}
	node.rightNode.use = true
	return node.rightNode, nil
}

func (node *Node) right(t *Tree) *Node {
	rightNode, err := node.getRightNode(t)
	if err != nil {
		panic(err)
	}
	return rightNode
}

func (node *Node) setRight(rightNode *Node) {
	node.RightNodeKey = rightNode.NodeKey
	node.rightNode = rightNode
}

// NOTE: mutates height and size
func (node *Node) calcHeightAndSize(t *Tree) error {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return err
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return err
	}

	node.SubtreeHeight = maxInt8(leftNode.SubtreeHeight, rightNode.SubtreeHeight) + 1
	node.Size = leftNode.Size + rightNode.Size
	return nil
}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

// NOTE: assumes that node can be modified
// TODO: optimize balance & rotate
func (tree *Tree) balance(node *Node) (newSelf *Node, err error) {
	if !node.NodeKey.IsEmpty() {
		return nil, fmt.Errorf("unexpected balance() call on persisted node")
	}
	balance, err := node.calcBalance(tree)
	if err != nil {
		return nil, err
	}

	if balance > 1 {
		lftBalance, err := node.left(tree).calcBalance(tree)
		if err != nil {
			return nil, err
		}

		if lftBalance >= 0 {
			// Left Left Case
			newNode, err := tree.rotateRight(node)
			if err != nil {
				return nil, err
			}
			return newNode, nil
		}
		// Left Right Case
		newLeftNode, err := tree.rotateLeft(node.left(tree))
		if err != nil {
			return nil, err
		}
		node.setLeft(newLeftNode)

		newNode, err := tree.rotateRight(node)
		if err != nil {
			return nil, err
		}

		return newNode, nil
	}
	if balance < -1 {
		rightNode, err := node.getRightNode(tree)
		if err != nil {
			return nil, err
		}

		rightBalance, err := rightNode.calcBalance(tree)
		if err != nil {
			return nil, err
		}
		if rightBalance <= 0 {
			// Right Right Case
			newNode, err := tree.rotateLeft(node)
			if err != nil {
				return nil, err
			}
			return newNode, nil
		}
		// Right Left Case
		newRightNode, err := tree.rotateRight(rightNode)
		if err != nil {
			return nil, err
		}
		node.setRight(newRightNode)

		newNode, err := tree.rotateLeft(node)
		if err != nil {
			return nil, err
		}
		return newNode, nil
	}
	// Nothing changed
	return node, nil
}

func (node *Node) calcBalance(t *Tree) (int, error) {
	leftNode, err := node.getLeftNode(t)
	if err != nil {
		return 0, err
	}

	rightNode, err := node.getRightNode(t)
	if err != nil {
		return 0, err
	}

	return int(leftNode.SubtreeHeight) - int(rightNode.SubtreeHeight), nil
}

// Rotate right and return the new node and orphan.
func (tree *Tree) rotateRight(node *Node) (*Node, error) {
	var err error
	// TODO: optimize balance & rotate.
	tree.addOrphan(node)
	tree.mutateNode(node)

	tree.addOrphan(node.left(tree))
	newNode := node.left(tree)
	tree.mutateNode(newNode)

	node.setLeft(newNode.right(tree))
	newNode.setRight(node)

	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, err
	}

	err = newNode.calcHeightAndSize(tree)
	if err != nil {
		return nil, err
	}

	return newNode, nil
}

// Rotate left and return the new node and orphan.
func (tree *Tree) rotateLeft(node *Node) (*Node, error) {
	var err error
	// TODO: optimize balance & rotate.
	tree.addOrphan(node)
	tree.mutateNode(node)

	tree.addOrphan(node.right(tree))
	newNode := node.right(tree)
	tree.mutateNode(newNode)

	node.setRight(newNode.left(tree))
	newNode.setLeft(node)

	err = node.calcHeightAndSize(tree)
	if err != nil {
		return nil, err
	}

	err = newNode.calcHeightAndSize(tree)
	if err != nil {
		return nil, err
	}

	return newNode, nil
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *Node) _hash(version int64) []byte {
	if node.hash != nil {
		return node.hash
	}

	h := sha256.New()
	if err := node.writeHashBytes(h, version); err != nil {
		return nil
	}
	node.hash = h.Sum(nil)

	return node.hash
}

func (node *Node) writeHashBytes(w io.Writer, version int64) error {
	var (
		n   int
		buf [binary.MaxVarintLen64]byte
	)

	n = binary.PutVarint(buf[:], int64(node.SubtreeHeight))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	n = binary.PutVarint(buf[:], node.Size)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	n = binary.PutVarint(buf[:], version)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.isLeaf() {
		if err := EncodeBytes(w, node.Key); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.Value)

		if err := EncodeBytes(w, valueHash[:]); err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		if err := EncodeBytes(w, node.leftNode.hash); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		if err := EncodeBytes(w, node.rightNode.hash); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

func (node *Node) isLeaf() bool {
	return node.SubtreeHeight == 0
}

func (node *Node) clear() {
	node.Key = nil
	node.Value = nil
	node.hash = nil
	node.NodeKey = NodeKey{}
	node.leftNode = nil
	node.rightNode = nil
	node.RightNodeKey = NodeKey{}
	node.LeftNodeKey = NodeKey{}
	node.SubtreeHeight = 0
	node.Size = 0
	node.use = false
}

// EncodeBytes writes a varint length-prefixed byte slice to the writer,
// it's used for hash computation, must be compatible with the official IAVL implementation.
func EncodeBytes(w io.Writer, bz []byte) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(bz)))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	_, err := w.Write(bz)
	return err
}

// MakeNode constructs a *Node from an encoded byte slice.
func MakeNode(nodeKey, buf []byte) (*Node, error) {
	// Read node header (height, size, version, key).
	height, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.height, %w", err)
	}
	buf = buf[n:]
	if height < int64(math.MinInt8) || height > int64(math.MaxInt8) {
		return nil, errors.New("invalid height, must be int8")
	}

	size, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.size, %w", err)
	}
	buf = buf[n:]

	key, n, err := encoding.DecodeBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.key, %w", err)
	}
	buf = buf[n:]

	node := &Node{
		SubtreeHeight: int8(height),
		NodeKey:       NodeKey{},
		Size:          size,
		Key:           key,
	}
	copy(node.NodeKey[:], nodeKey)

	// Read node body.

	if node.isLeaf() {
		val, _, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.value, %w", err)
		}
		node.Value = val
		node._hash(node.NodeKey.Version())
	} else { // Read children.
		leftNodeKey, n, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.leftHash, %w", err)
		}
		buf = buf[n:]

		rightNodeKey, _, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.rightHash, %w", err)
		}
		node.LeftNodeKey = NodeKey{}
		node.RightNodeKey = NodeKey{}
		copy(node.LeftNodeKey[:], leftNodeKey)
		copy(node.RightNodeKey[:], rightNodeKey)
	}
	return node, nil
}

func (node *Node) WriteBytes(w io.Writer) error {
	if node == nil {
		return errors.New("cannot write nil node")
	}
	cause := encoding.EncodeVarint(w, int64(node.SubtreeHeight))
	if cause != nil {
		return fmt.Errorf("writing height; %w", cause)
	}
	cause = encoding.EncodeVarint(w, node.Size)
	if cause != nil {
		return fmt.Errorf("writing size; %w", cause)
	}

	// Unlike writeHashBytes, key is written for inner nodes.
	cause = encoding.EncodeBytes(w, node.Key)
	if cause != nil {
		return fmt.Errorf("writing key; %w", cause)
	}

	if node.isLeaf() {
		cause = encoding.EncodeBytes(w, node.Value)
		if cause != nil {
			return fmt.Errorf("writing value; %w", cause)
		}
	} else {
		if node.LeftNodeKey.IsEmpty() {
			return fmt.Errorf("left node key is nil")
		}
		cause = encoding.EncodeBytes(w, node.LeftNodeKey[:])
		if cause != nil {
			return fmt.Errorf("writing left node key; %w", cause)
		}

		if node.RightNodeKey.IsEmpty() {
			return fmt.Errorf("right node key is nil")
		}
		cause = encoding.EncodeBytes(w, node.RightNodeKey[:])
		if cause != nil {
			return fmt.Errorf("writing right node key; %w", cause)
		}
	}
	return nil
}

func (node *Node) Bytes() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := node.WriteBytes(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
