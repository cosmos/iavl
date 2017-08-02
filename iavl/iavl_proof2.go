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
	Before   *PathToKey
	After    *PathToKey
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

// ---------- New interface -----------

func (node *IAVLNode) constructKeyExistsProof(t *IAVLTree, key []byte, valuePtr *[]byte, proof *KeyExistsProof) (exists bool) {
	if node.height == 0 {
		if bytes.Compare(node.key, key) == 0 {
			*valuePtr = node.value
			proof.LeafHash = node.hash
			return true
		} else {
			return false
		}
	} else {
		if bytes.Compare(key, node.key) < 0 {
			exists := node.getLeftNode(t).constructKeyExistsProof(t, key, valuePtr, proof)
			if !exists {
				return false
			}
			branch := IAVLProofInnerNode{
				Height: node.height,
				Size:   node.size,
				Left:   nil,
				Right:  node.getRightNode(t).hash,
			}
			proof.InnerNodes = append(proof.InnerNodes, branch)
			return true
		} else {
			exists := node.getRightNode(t).constructKeyExistsProof(t, key, valuePtr, proof)
			if !exists {
				return false
			}
			branch := IAVLProofInnerNode{
				Height: node.height,
				Size:   node.size,
				Left:   node.getLeftNode(t).hash,
				Right:  nil,
			}
			proof.InnerNodes = append(proof.InnerNodes, branch)
			return true
		}
	}
}

func (t *IAVLTree) ConstructKeyExistsProof(key []byte) (value []byte, proof *KeyExistsProof) {
	if t.root == nil {
		return nil, nil
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof = &KeyExistsProof{
		RootHash: t.root.hash,
	}
	if t.root.constructKeyExistsProof(t, key, &value, proof) {
		return value, proof
	}
	return nil, nil
}
