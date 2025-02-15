package iavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	encoding "github.com/cosmos/iavl/v2/internal"
	ics23 "github.com/cosmos/ics23/go"
)

var proofBufPool = &sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (tree *Tree) GetProof(version int64, key []byte) (proof *ics23.CommitmentProof, err error) {
	t, err := tree.ReadonlyClone()
	if err != nil {
		return nil, err
	}
	defer func() {
		dErr := t.sql.Close()
		if dErr != nil {
			err = errors.Join(err, dErr)
		}
	}()
	if err := t.LoadVersion(version); err != nil {
		return nil, err
	}
	return t.getProof(key)
}

func (tree *Tree) getProof(key []byte) (proof *ics23.CommitmentProof, err error) {
	if tree.root == nil {
		return nil, errors.New("cannot get proof from empty tree")
	}
	if len(key) == 0 {
		return nil, errors.New("cannot get proof for empty key")
	}
	exists, err := tree.Has(key)
	if err != nil {
		return nil, err
	}
	if exists {
		existenceProof, err := tree.getExistenceProof(key)
		if err != nil {
			return nil, err
		}
		return &ics23.CommitmentProof{
			Proof: &ics23.CommitmentProof_Exist{
				Exist: existenceProof,
			},
		}, nil
	}
	return tree.getNonMembershipProof(key)
}

func (tree *Tree) getExistenceProof(key []byte) (proof *ics23.ExistenceProof, err error) {
	tree.Hash()
	path, node, err := tree.PathToLeaf(tree.root, key)
	return &ics23.ExistenceProof{
		Key:   node.key,
		Value: node.value,
		Leaf:  convertLeafOp(node.Version()),
		Path:  convertInnerOps(path),
	}, err
}

func (tree *Tree) getNonMembershipProof(key []byte) (*ics23.CommitmentProof, error) {
	// idx is one node right of what we want....
	var err error
	idx, val, err := tree.GetWithIndex(key)
	if err != nil {
		return nil, err
	}

	if val != nil {
		return nil, fmt.Errorf("cannot create NonExistanceProof when Key in State")
	}

	nonexist := &ics23.NonExistenceProof{
		Key: key,
	}

	if idx >= 1 {
		leftkey, _, err := tree.GetByIndex(idx - 1)
		if err != nil {
			return nil, err
		}

		nonexist.Left, err = tree.getExistenceProof(leftkey)
		if err != nil {
			return nil, err
		}
	}

	// this will be nil if nothing right of the queried key
	rightkey, _, err := tree.GetByIndex(idx)
	if err != nil {
		return nil, err
	}

	if rightkey != nil {
		nonexist.Right, err = tree.getExistenceProof(rightkey)
		if err != nil {
			return nil, err
		}
	}

	proof := &ics23.CommitmentProof{
		Proof: &ics23.CommitmentProof_Nonexist{
			Nonexist: nonexist,
		},
	}
	return proof, nil
}

func convertLeafOp(version int64) *ics23.LeafOp {
	var varintBuf [binary.MaxVarintLen64]byte
	// this is adapted from iavl/proof.go:proofLeafNode.Hash()
	prefix := convertVarIntToBytes(0, varintBuf)
	prefix = append(prefix, convertVarIntToBytes(1, varintBuf)...)
	prefix = append(prefix, convertVarIntToBytes(version, varintBuf)...)

	return &ics23.LeafOp{
		Hash:         ics23.HashOp_SHA256,
		PrehashValue: ics23.HashOp_SHA256,
		Length:       ics23.LengthOp_VAR_PROTO,
		Prefix:       prefix,
	}
}

// we cannot get the proofInnerNode type, so we need to do the whole path in one function
func convertInnerOps(path PathToLeaf) []*ics23.InnerOp {
	steps := make([]*ics23.InnerOp, 0, len(path))

	// lengthByte is the length prefix prepended to each of the sha256 sub-hashes
	var lengthByte byte = 0x20

	var varintBuf [binary.MaxVarintLen64]byte

	// we need to go in reverse order, iavl starts from root to leaf,
	// we want to go up from the leaf to the root
	for i := len(path) - 1; i >= 0; i-- {
		// this is adapted from iavl/proof.go:proofInnerNode.Hash()
		prefix := convertVarIntToBytes(int64(path[i].Height), varintBuf)
		prefix = append(prefix, convertVarIntToBytes(path[i].Size, varintBuf)...)
		prefix = append(prefix, convertVarIntToBytes(path[i].Version, varintBuf)...)

		var suffix []byte
		if len(path[i].Left) > 0 {
			// length prefixed left side
			prefix = append(prefix, lengthByte)
			prefix = append(prefix, path[i].Left...)
			// prepend the length prefix for child
			prefix = append(prefix, lengthByte)
		} else {
			// prepend the length prefix for child
			prefix = append(prefix, lengthByte)
			// length-prefixed right side
			suffix = []byte{lengthByte}
			suffix = append(suffix, path[i].Right...)
		}

		op := &ics23.InnerOp{
			Hash:   ics23.HashOp_SHA256,
			Prefix: prefix,
			Suffix: suffix,
		}
		steps = append(steps, op)
	}
	return steps
}

func convertVarIntToBytes(orig int64, buf [binary.MaxVarintLen64]byte) []byte {
	n := binary.PutVarint(buf[:], orig)
	return buf[:n]
}

// If the key does not exist, returns the path to the next leaf left of key (w/
// path), except when key is less than the least item, in which case it returns
// a path to the least item.
func (tree *Tree) PathToLeaf(node *Node, key []byte) (PathToLeaf, *Node, error) {
	path := new(PathToLeaf)
	val, err := tree.pathToLeaf(node, key, path)
	return *path, val, err
}

// pathToLeaf is a helper which recursively constructs the PathToLeaf.
// As an optimization the already constructed path is passed in as an argument
// and is shared among recursive calls.
func (tree *Tree) pathToLeaf(node *Node, key []byte, path *PathToLeaf) (*Node, error) {
	if node.subtreeHeight == 0 {
		if bytes.Equal(node.key, key) {
			return node, nil
		}
		return node, errors.New("key does not exist")
	}

	// Note that we do not store the left child in the ProofInnerNode when we're going to add the
	// left node as part of the path, similarly we don'tree store the right child info when going down
	// the right child node. This is done as an optimization since the child info is going to be
	// already stored in the next ProofInnerNode in PathToLeaf.
	if bytes.Compare(key, node.key) < 0 {
		// left side
		rightNode, err := node.getRightNode(tree.sql)
		if err != nil {
			return nil, err
		}

		pin := ProofInnerNode{
			Height:  node.subtreeHeight,
			Size:    node.size,
			Version: node.Version(),
			Left:    nil,
			Right:   rightNode.hash,
		}
		*path = append(*path, pin)

		leftNode, err := node.getLeftNode(tree.sql)
		if err != nil {
			return nil, err
		}
		n, err := tree.pathToLeaf(leftNode, key, path)
		return n, err
	}
	// right side
	leftNode, err := node.getLeftNode(tree.sql)
	if err != nil {
		return nil, err
	}

	pin := ProofInnerNode{
		Height:  node.subtreeHeight,
		Size:    node.size,
		Version: node.Version(),
		Left:    leftNode.hash,
		Right:   nil,
	}
	*path = append(*path, pin)

	rightNode, err := node.getRightNode(tree.sql)
	if err != nil {
		return nil, err
	}

	n, err := tree.pathToLeaf(rightNode, key, path)
	return n, err
}

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

func (pin ProofInnerNode) Hash(childHash []byte) ([]byte, error) {
	hasher := sha256.New()

	buf := proofBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer proofBufPool.Put(buf)

	err := encoding.EncodeVarint(buf, int64(pin.Height))
	if err == nil {
		err = encoding.EncodeVarint(buf, pin.Size)
	}
	if err == nil {
		err = encoding.EncodeVarint(buf, pin.Version)
	}

	if len(pin.Left) > 0 && len(pin.Right) > 0 {
		return nil, errors.New("both left and right child hashes are set")
	}

	if len(pin.Left) == 0 {
		if err == nil {
			err = encoding.EncodeBytes(buf, childHash)
		}
		if err == nil {
			err = encoding.EncodeBytes(buf, pin.Right)
		}
	} else {
		if err == nil {
			err = encoding.EncodeBytes(buf, pin.Left)
		}
		if err == nil {
			err = encoding.EncodeBytes(buf, childHash)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to hash ProofInnerNode: %v", err)
	}

	_, err = hasher.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

//----------------------------------------

type ProofLeafNode struct {
	Key       []byte `json:"key"`
	ValueHash []byte `json:"value"`
	Version   int64  `json:"version"`
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

func (pln ProofLeafNode) Hash() ([]byte, error) {
	hasher := sha256.New()

	buf := proofBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer proofBufPool.Put(buf)

	err := encoding.EncodeVarint(buf, 0)
	if err == nil {
		err = encoding.EncodeVarint(buf, 1)
	}
	if err == nil {
		err = encoding.EncodeVarint(buf, pln.Version)
	}
	if err == nil {
		err = encoding.EncodeBytes(buf, pln.Key)
	}
	if err == nil {
		err = encoding.EncodeBytes(buf, pln.ValueHash)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to hash ProofLeafNode: %v", err)
	}
	_, err = hasher.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}

	return hasher.Sum(nil), nil
}

// PathToLeaf represents an inner path to a leaf node.
// Note that the nodes are ordered such that the last one is closest
// to the root of the tree.
type PathToLeaf []ProofInnerNode

func (pl PathToLeaf) String() string {
	return pl.stringIndented("")
}

func (pl PathToLeaf) stringIndented(indent string) string {
	if len(pl) == 0 {
		return "empty-PathToLeaf"
	}
	strs := make([]string, 0, len(pl))
	for i, pin := range pl {
		if i == 20 {
			strs = append(strs, fmt.Sprintf("... (%v total)", len(pl)))
			break
		}
		strs = append(strs, fmt.Sprintf("%v:%v", i, pin.stringIndented(indent+"  ")))
	}
	return fmt.Sprintf(`PathToLeaf{
%s  %v
%s}`,
		indent, strings.Join(strs, "\n"+indent+"  "),
		indent)
}

// returns -1 if invalid.
func (pl PathToLeaf) Index() (idx int64) {
	for i, node := range pl {
		switch {
		case node.Left == nil:
			continue
		case node.Right == nil:
			if i < len(pl)-1 {
				idx += node.Size - pl[i+1].Size
			} else {
				idx += node.Size - 1
			}
		default:
			return -1
		}
	}
	return idx
}
