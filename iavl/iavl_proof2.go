package iavl

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/tendermint/go-wire"
)

type PathToKey struct {
	LeafHash   []byte
	InnerNodes []IAVLProofInnerNode
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
	leafHash := leafNode.Hash()
	if !bytes.Equal(leafHash, proof.LeafHash) {
		return errors.Errorf("leaf hash does not match %x != %x", leafHash, proof.LeafHash)
	}
	hash := leafHash
	for _, branch := range proof.InnerNodes {
		hash = branch.Hash(hash)
	}
	if !bytes.Equal(proof.RootHash, hash) {
		return errors.New("path does not match root")
	}
	return nil
}

type KeyNotExistsProof struct {
	RootHash []byte

	LeftPath *PathToKey
	LeftNode IAVLProofLeafNode

	RightPath *PathToKey
	RightNode IAVLProofLeafNode
}

func (proof *KeyNotExistsProof) Verify(key []byte, value []byte, root []byte) bool {
	return false
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
		return errors.Errorf("couldn't construct non-existence proof: key '%x' exists", key)
	}

	// TODO: Take care of out-of bounds indices.
	lkey, lval := t.GetByIndex(idx - 1)
	rkey, rval := t.GetByIndex(idx + 1)

	if lkey == nil || rkey == nil {
		return errors.New("couldn't get keys required for non-existence proof")
	}

	lproof := &KeyExistsProof{
		RootHash: t.root.hash,
	}
	node.constructKeyExistsProof(t, lkey, lproof)
	proof.LeftPath = &lproof.PathToKey
	proof.LeftNode = IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval}

	rproof := &KeyExistsProof{
		RootHash: t.root.hash,
	}
	node.constructKeyExistsProof(t, rkey, rproof)
	proof.RightPath = &rproof.PathToKey
	proof.RightNode = IAVLProofLeafNode{KeyBytes: rkey, ValueBytes: rval}

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
