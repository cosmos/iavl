package iavl

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"

	"github.com/pkg/errors"

	cmn "github.com/cosmos/iavl/common"
	iavlproto "github.com/cosmos/iavl/proto"
)

var (
	// ErrInvalidProof is returned by Verify when a proof cannot be validated.
	ErrInvalidProof = fmt.Errorf("invalid proof")

	// ErrInvalidInputs is returned when the inputs passed to the function are invalid.
	ErrInvalidInputs = fmt.Errorf("invalid inputs")

	// ErrInvalidRoot is returned when the root passed in does not match the proof's.
	ErrInvalidRoot = fmt.Errorf("invalid root")
)

//----------------------------------------

type ProofInnerNode struct {
	Height  int8   `json:"height"`
	Size    int64  `json:"size"`
	Version int64  `json:"version"`
	Left    []byte `json:"left"`
	Right   []byte `json:"right"`
}

func (pin ProofInnerNode) String() string {
	return pin.stringIndented("")
}

func (pin ProofInnerNode) stringIndented(indent string) string {
	return fmt.Sprintf(`ProofInnerNode{
%s  Height:  %v
%s  Size:    %v
%s  Version: %v
%s  Left:    %X
%s  Right:   %X
%s}`,
		indent, pin.Height,
		indent, pin.Size,
		indent, pin.Version,
		indent, pin.Left,
		indent, pin.Right,
		indent)
}

func (pin ProofInnerNode) Hash(childHash []byte) []byte {
	hasher := sha256.New()
	buf := new(bytes.Buffer)

	err := encodeVarint(buf, int64(pin.Height))
	if err == nil {
		err = encodeVarint(buf, pin.Size)
	}
	if err == nil {
		err = encodeVarint(buf, pin.Version)
	}

	if len(pin.Left) == 0 {
		if err == nil {
			err = encodeBytes(buf, childHash)
		}
		if err == nil {
			err = encodeBytes(buf, pin.Right)
		}
	} else {
		if err == nil {
			err = encodeBytes(buf, pin.Left)
		}
		if err == nil {
			err = encodeBytes(buf, childHash)
		}
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to hash ProofInnerNode: %v", err))
	}

	_, err = hasher.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
	return hasher.Sum(nil)
}

// toProto converts the inner node proof to Protobuf, for use in ProofOps.
func (pin ProofInnerNode) toProto() *iavlproto.ProofInnerNode {
	return &iavlproto.ProofInnerNode{
		Height:  int32(pin.Height),
		Size_:   pin.Size,
		Version: pin.Version,
		Left:    pin.Left,
		Right:   pin.Right,
	}
}

// proofInnerNodeFromProto converts a Protobuf ProofInnerNode to a ProofInnerNode.
func proofInnerNodeFromProto(pbInner *iavlproto.ProofInnerNode) (ProofInnerNode, error) {
	if pbInner == nil {
		return ProofInnerNode{}, errors.New("inner node cannot be nil")
	}
	if pbInner.Height > math.MaxInt8 || pbInner.Height < math.MinInt8 {
		return ProofInnerNode{}, fmt.Errorf("height must fit inside an int8, got %v", pbInner.Height)
	}
	return ProofInnerNode{
		Height:  int8(pbInner.Height),
		Size:    pbInner.Size_,
		Version: pbInner.Version,
		Left:    pbInner.Left,
		Right:   pbInner.Right,
	}, nil
}

//----------------------------------------

type ProofLeafNode struct {
	Key       cmn.HexBytes `json:"key"`
	ValueHash cmn.HexBytes `json:"value"`
	Version   int64        `json:"version"`
}

func (pln ProofLeafNode) String() string {
	return pln.stringIndented("")
}

func (pln ProofLeafNode) stringIndented(indent string) string {
	return fmt.Sprintf(`ProofLeafNode{
%s  Key:       %v
%s  ValueHash: %X
%s  Version:   %v
%s}`,
		indent, pln.Key,
		indent, pln.ValueHash,
		indent, pln.Version,
		indent)
}

func (pln ProofLeafNode) Hash() []byte {
	hasher := sha256.New()
	buf := new(bytes.Buffer)

	err := encodeVarint(buf, 0)
	if err == nil {
		err = encodeVarint(buf, 1)
	}
	if err == nil {
		err = encodeVarint(buf, pln.Version)
	}
	if err == nil {
		err = encodeBytes(buf, pln.Key)
	}
	if err == nil {
		err = encodeBytes(buf, pln.ValueHash)
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to hash ProofLeafNode: %v", err))
	}
	_, err = hasher.Write(buf.Bytes())
	if err != nil {
		panic(err)

	}

	return hasher.Sum(nil)
}

// toProto converts the leaf node proof to Protobuf, for use in ProofOps.
func (pln ProofLeafNode) toProto() *iavlproto.ProofLeafNode {
	return &iavlproto.ProofLeafNode{
		Key:       pln.Key,
		ValueHash: pln.ValueHash,
		Version:   pln.Version,
	}
}

// proofLeafNodeFromProto converts a Protobuf ProofLeadNode to a ProofLeafNode.
func proofLeafNodeFromProto(pbLeaf *iavlproto.ProofLeafNode) (ProofLeafNode, error) {
	if pbLeaf == nil {
		return ProofLeafNode{}, errors.New("leaf node cannot be nil")
	}
	return ProofLeafNode{
		Key:       pbLeaf.Key,
		ValueHash: pbLeaf.ValueHash,
		Version:   pbLeaf.Version,
	}, nil
}

//----------------------------------------

// If the key does not exist, returns the path to the next leaf left of key (w/
// path), except when key is less than the least item, in which case it returns
// a path to the least item.
func (node *Node) PathToLeaf(t *ImmutableTree, key []byte) (PathToLeaf, *Node, error) {
	path := new(PathToLeaf)
	val, err := node.pathToLeaf(t, key, path)
	return *path, val, err
}

// pathToLeaf is a helper which recursively constructs the PathToLeaf.
// As an optimization the already constructed path is passed in as an argument
// and is shared among recursive calls.
func (node *Node) pathToLeaf(t *ImmutableTree, key []byte, path *PathToLeaf) (*Node, error) {
	if node.height == 0 {
		if bytes.Equal(node.key, key) {
			return node, nil
		}
		return node, errors.New("key does not exist")
	}

	// Note that we do not store the left child in the ProofInnerNode when we're going to add the
	// left node as part of the path, similarly we don't store the right child info when going down
	// the right child node. This is done as an optimization since the child info is going to be
	// already stored in the next ProofInnerNode in PathToLeaf.
	if bytes.Compare(key, node.key) < 0 {
		// left side
		pin := ProofInnerNode{
			Height:  node.height,
			Size:    node.size,
			Version: node.version,
			Left:    nil,
			Right:   node.getRightNode(t).hash,
		}
		*path = append(*path, pin)
		n, err := node.getLeftNode(t).pathToLeaf(t, key, path)
		return n, err
	}
	// right side
	pin := ProofInnerNode{
		Height:  node.height,
		Size:    node.size,
		Version: node.version,
		Left:    node.getLeftNode(t).hash,
		Right:   nil,
	}
	*path = append(*path, pin)
	n, err := node.getRightNode(t).pathToLeaf(t, key, path)
	return n, err
}
