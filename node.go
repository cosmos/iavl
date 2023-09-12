package v6

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
)

const nodeKeySize = 12

type nodeKey [nodeKeySize]byte

func newNodeKey(version int64, sequence uint32) *nodeKey {
	nk := nodeKey{}
	binary.BigEndian.PutUint64(nk[:], uint64(version))
	binary.BigEndian.PutUint32(nk[8:], sequence)
	return &nk
}

func (nk *nodeKey) Version() int64 {
	return int64(binary.BigEndian.Uint64(nk[:]))
}

func (nk *nodeKey) Sequence() uint32 {
	return binary.BigEndian.Uint32(nk[8:])
}

// Node represents a node in a Tree.
type Node struct {
	key           []byte
	value         []byte
	hash          []byte
	nodeKey       *nodeKey
	leftNodeKey   *nodeKey
	rightNodeKey  *nodeKey
	size          int64
	leftNode      *Node
	rightNode     *Node
	subtreeHeight int8

	frameId  int
	use      bool
	dirty    bool
	overflow bool
	lock     bool
}

// String returns a string representation of the node key.
func (nk *nodeKey) String() string {
	return fmt.Sprintf("(%d, %d)", nk.Version(), nk.Sequence())
}

// getLeftNode will never be called on leaf nodes. all tree nodes have 2 children.
func (node *Node) getLeftNode(t *Tree) (*Node, error) {
	if node.isLeaf() {
		return nil, fmt.Errorf("leaf node has no left node")
	}
	//if node.leftNode == nil {
	//	return nil, fmt.Errorf("left node is nil")
	//}
	if node.leftNode == nil || node.leftNodeKey != node.leftNode.nodeKey {
		if node.leftNodeKey == nil {
			return nil, fmt.Errorf("left node key is nil")
		}
		node.leftNode = t.db.Get(*node.leftNodeKey)
		if node.leftNode == nil {
			return nil, fmt.Errorf("left node is nil; fetch failed")
		}
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
	node.leftNodeKey = leftNode.nodeKey
	node.leftNode = leftNode
}

func (node *Node) getRightNode(t *Tree) (*Node, error) {
	if node.isLeaf() {
		return nil, fmt.Errorf("leaf node has no right node")
	}
	if node.rightNode == nil || node.rightNodeKey != node.rightNode.nodeKey {
		// return nil, fmt.Errorf("right node key mismatch; expected %v, got %v",
		//	node.rightNodeKey, node.rightNode.nodeKey)
		if node.rightNodeKey == nil {
			return nil, fmt.Errorf("right node key is nil")
		}
		node.rightNode = t.db.Get(*node.rightNodeKey)
		if node.rightNode == nil {
			return nil, fmt.Errorf("right node is nil; fetch failed")
		}
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
	node.rightNodeKey = rightNode.nodeKey
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

	node.subtreeHeight = maxInt8(leftNode.subtreeHeight, rightNode.subtreeHeight) + 1
	node.size = leftNode.size + rightNode.size
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
	if node.nodeKey != nil {
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

	return int(leftNode.subtreeHeight) - int(rightNode.subtreeHeight), nil
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

	n = binary.PutVarint(buf[:], int64(node.subtreeHeight))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	n = binary.PutVarint(buf[:], node.size)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	n = binary.PutVarint(buf[:], version)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.isLeaf() {
		if err := EncodeBytes(w, node.key); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

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
	return node.subtreeHeight == 0
}

// EncodeBytes writes a varint length-prefixed byte slice to the writer,
// it's used for hash computation, must be compactible with the official IAVL implementation.
func EncodeBytes(w io.Writer, bz []byte) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(bz)))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	_, err := w.Write(bz)
	return err
}

/*
// MakeNode constructs an *Node from an encoded byte slice.
func MakeNode(nk, buf []byte) (*Node, error) {
	// Read node header (height, size, key).
	height, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.height, %w", err)
	}
	buf = buf[n:]
	height8 := int8(height)
	if height != int64(height8) {
		return nil, errors.New("invalid height, out of int8 range")
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
		subtreeHeight: height8,
		size:          size,
		key:           key,
	}
	copy(node.nodeKey[:], nk)

	// Read node body.
	if node.isLeaf() {
		val, _, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.value, %w", err)
		}
		node.value = val
		// ensure take the hash for the leaf node
		node._hash(node.nodeKey.Version())
	} else { // Read children.
		node.hash, n, err = encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.hash, %w", err)
		}
		buf = buf[n:]

		mode, n, err := encoding.DecodeVarint(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding mode, %w", err)
		}
		buf = buf[n:]
		if mode < 0 || mode > 3 {
			return nil, errors.New("invalid mode")
		}

		if mode&ModeLegacyLeftNode != 0 { // legacy leftNodeKey
			node.leftNodeKey, n, err = encoding.DecodeBytes(buf)
			if err != nil {
				return nil, fmt.Errorf("decoding legacy node.leftNodeKey, %w", err)
			}
			buf = buf[n:]
		} else {
			var (
				leftNodeKey NodeKey
				nonce       int64
			)
			leftNodeKey.version, n, err = encoding.DecodeVarint(buf)
			if err != nil {
				return nil, fmt.Errorf("decoding node.leftNodeKey.version, %w", err)
			}
			buf = buf[n:]
			nonce, n, err = encoding.DecodeVarint(buf)
			if err != nil {
				return nil, fmt.Errorf("decoding node.leftNodeKey.nonce, %w", err)
			}
			buf = buf[n:]
			leftNodeKey.nonce = uint32(nonce)
			if nonce != int64(leftNodeKey.nonce) {
				return nil, errors.New("invalid leftNodeKey.nonce, out of int32 range")
			}
			node.leftNodeKey = leftNodeKey.GetKey()
		}
		if mode&ModeLegacyRightNode != 0 { // legacy rightNodeKey
			node.rightNodeKey, _, err = encoding.DecodeBytes(buf)
			if err != nil {
				return nil, fmt.Errorf("decoding legacy node.rightNodeKey, %w", err)
			}
		} else {
			var (
				rightNodeKey NodeKey
				nonce        int64
			)
			rightNodeKey.version, n, err = encoding.DecodeVarint(buf)
			if err != nil {
				return nil, fmt.Errorf("decoding node.rightNodeKey.version, %w", err)
			}
			buf = buf[n:]
			nonce, _, err = encoding.DecodeVarint(buf)
			if err != nil {
				return nil, fmt.Errorf("decoding node.rightNodeKey.nonce, %w", err)
			}
			rightNodeKey.nonce = uint32(nonce)
			if nonce != int64(rightNodeKey.nonce) {
				return nil, errors.New("invalid rightNodeKey.nonce, out of int32 range")
			}
			node.rightNodeKey = rightNodeKey.GetKey()
		}
	}
	return node, nil
}

// MakeLegacyNode constructs a legacy *Node from an encoded byte slice.
func MakeLegacyNode(hash, buf []byte) (*Node, error) {
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

	ver, n, err := encoding.DecodeVarint(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.version, %w", err)
	}
	buf = buf[n:]

	key, n, err := encoding.DecodeBytes(buf)
	if err != nil {
		return nil, fmt.Errorf("decoding node.key, %w", err)
	}
	buf = buf[n:]

	node := &Node{
		subtreeHeight: int8(height),
		size:          size,
		nodeKey:       &NodeKey{version: ver},
		key:           key,
		hash:          hash,
	}

	// Read node body.

	if node.isLeaf() {
		val, _, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.value, %w", err)
		}
		node.value = val
	} else { // Read children.
		leftHash, n, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.leftHash, %w", err)
		}
		buf = buf[n:]

		rightHash, _, err := encoding.DecodeBytes(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding node.rightHash, %w", err)
		}
		node.leftNodeKey = leftHash
		node.rightNodeKey = rightHash
	}
	return node, nil
}
*/
