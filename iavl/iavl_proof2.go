package iavl

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"github.com/tendermint/go-wire"
)

type PathToKey struct {
	LeafHash   []byte
	InnerNodes []IAVLProofInnerNode
}

func (p *PathToKey) String() string {
	str := ""
	for i := len(p.InnerNodes) - 1; i >= 0; i-- {
		str += p.InnerNodes[i].String() + "\n"
	}
	str += fmt.Sprintf("hash(leaf)=%x\n", p.LeafHash)
	return str
}

func (p *PathToKey) Verify(leafNode IAVLProofLeafNode, root []byte) error {
	leafHash := leafNode.Hash()
	if !bytes.Equal(leafHash, p.LeafHash) {
		return errors.Errorf("leaf hash does not match %x != %x", leafHash, p.LeafHash)
	}
	hash := leafHash
	for _, branch := range p.InnerNodes {
		hash = branch.Hash(hash)
	}
	if !bytes.Equal(root, hash) {
		return errors.New("path does not match supplied root")
	}
	return nil
}

func (p *PathToKey) IsLeftmost() bool {
	for _, node := range p.InnerNodes {
		if node.Left != nil {
			return false
		}
	}
	return true
}

func (p *PathToKey) IsRightmost() bool {
	for _, node := range p.InnerNodes {
		if node.Right != nil {
			return false
		}
	}
	return true
}

type KeyExistsProof struct {
	PathToKey
	RootHash []byte
}

func (proof *KeyExistsProof) Verify(key []byte, value []byte, root []byte) error {
	if !bytes.Equal(proof.RootHash, root) {
		return errors.New("roots are not equal")
	}
	leafNode := IAVLProofLeafNode{KeyBytes: key, ValueBytes: value}
	return proof.PathToKey.Verify(leafNode, root)
}

type KeyNotExistsProof struct {
	RootHash []byte

	LeftPath *PathToKey
	LeftNode IAVLProofLeafNode

	RightPath *PathToKey
	RightNode IAVLProofLeafNode
}

func (proof *KeyNotExistsProof) Verify(key []byte, root []byte) error {
	if !bytes.Equal(proof.RootHash, root) {
		return errors.New("roots do not match")
	}

	if proof.LeftPath == nil && proof.RightPath == nil {
		return errors.New("at least one path must exist")
	}
	if proof.LeftPath != nil {
		if err := proof.LeftPath.Verify(proof.LeftNode, root); err != nil {
			return errors.New("failed to verify left path")
		}
		if bytes.Compare(proof.LeftNode.KeyBytes, key) != -1 {
			return errors.New("left node key must be lesser than supplied key")
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.Verify(proof.RightNode, root); err != nil {
			return errors.New("failed to verify right path")
		}
		if bytes.Compare(proof.RightNode.KeyBytes, key) != 1 {
			return errors.New("left node key must be greater than supplied key")
		}
	}

	// Both paths exist, check that they are sequential.
	if proof.RightPath != nil && proof.LeftPath != nil {
		// TODO
		return nil
	}

	// Only right path exists, check that node is at left boundary.
	if proof.LeftPath == nil {
		if !proof.RightPath.IsLeftmost() {
			return errors.New("right path is only one but not leftmost")
		}
	}

	// Only left path exists, check that node is at right boundary.
	if proof.RightPath == nil {
		if !proof.LeftPath.IsRightmost() {
			return errors.New("left path is only one but not rightmost")
		}
	}

	return nil
}

type KeyRangeExistsProof struct {
	RootHash   []byte
	PathToKeys []*PathToKey
}

func (proof *KeyRangeExistsProof) Verify(key []byte, value []byte, root []byte) bool {
	return false
}

func ReadKeyExistsProof(data []byte) (*KeyExistsProof, error) {
	proof := new(KeyExistsProof)
	err := wire.ReadBinaryBytes(data, &proof)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (node *IAVLNode) constructKeyExistsProof(t *IAVLTree, key []byte, proof *KeyExistsProof) (value []byte, exists bool) {
	if node.height == 0 {
		if bytes.Compare(node.key, key) == 0 {
			proof.LeafHash = node.hash
			return node.value, true
		}
		return nil, false
	}

	if bytes.Compare(key, node.key) < 0 {
		if value, exists := node.getLeftNode(t).constructKeyExistsProof(t, key, proof); exists {
			branch := IAVLProofInnerNode{
				Height: node.height,
				Size:   node.size,
				Left:   nil,
				Right:  node.getRightNode(t).hash,
			}
			proof.InnerNodes = append(proof.InnerNodes, branch)
			return value, true
		}
		return nil, false
	}

	if value, exists = node.getRightNode(t).constructKeyExistsProof(t, key, proof); exists {
		branch := IAVLProofInnerNode{
			Height: node.height,
			Size:   node.size,
			Left:   node.getLeftNode(t).hash,
			Right:  nil,
		}
		proof.InnerNodes = append(proof.InnerNodes, branch)
		return value, true
	}
	return nil, false
}

func (node *IAVLNode) constructKeyNotExistsProof(t *IAVLTree, key []byte, proof *KeyNotExistsProof) error {
	idx, _, exists := t.Get(key)
	if exists {
		return errors.Errorf("couldn't construct non-existence proof: key 0x%x exists", key)
	}

	var (
		lkey, lval []byte
		rkey, rval []byte
	)
	if idx > 0 {
		lkey, lval = t.GetByIndex(idx - 1)
	}
	if idx < t.Size()-1 {
		rkey, rval = t.GetByIndex(idx + 1)
	}

	// TODO: What if tree is empty?
	if lkey == nil && rkey == nil {
		return errors.New("couldn't get keys required for non-existence proof")
	}

	if lkey != nil {
		lproof := &KeyExistsProof{
			RootHash: t.root.hash,
		}
		node.constructKeyExistsProof(t, lkey, lproof)
		proof.LeftPath = &lproof.PathToKey
		proof.LeftNode = IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval}
	}
	if rkey != nil {
		rproof := &KeyExistsProof{
			RootHash: t.root.hash,
		}
		node.constructKeyExistsProof(t, rkey, rproof)
		proof.RightPath = &rproof.PathToKey
		proof.RightNode = IAVLProofLeafNode{KeyBytes: rkey, ValueBytes: rval}
	}

	return nil
}

func (t *IAVLTree) ConstructKeyExistsProof(key []byte) (value []byte, proof *KeyExistsProof) {
	if t.root == nil {
		return nil, nil
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof = &KeyExistsProof{
		RootHash: t.root.hash,
	}
	if value, exists := t.root.constructKeyExistsProof(t, key, proof); exists {
		return value, proof
	}
	return nil, nil
}

func (t *IAVLTree) ConstructKeyNotExistsProof(key []byte) (*KeyNotExistsProof, error) {
	if t.root == nil {
		return nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof := &KeyNotExistsProof{
		RootHash: t.root.hash,
	}
	if err := t.root.constructKeyNotExistsProof(t, key, proof); err != nil {
		return nil, err
	}
	return proof, nil
}
