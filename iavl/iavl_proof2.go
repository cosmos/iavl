package iavl

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"github.com/tendermint/go-wire/data"
)

type PathToKey struct {
	LeafHash   data.Bytes           `json:"leaf_hash"`
	InnerNodes []IAVLProofInnerNode `json:"inner_nodes"`
}

func (p *PathToKey) String() string {
	str := ""
	for i := len(p.InnerNodes) - 1; i >= 0; i-- {
		str += p.InnerNodes[i].String() + "\n"
	}
	str += fmt.Sprintf("hash(leaf)=%x\n", p.LeafHash)
	return str
}

func (p *PathToKey) verify(leafNode IAVLProofLeafNode, root []byte) error {
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

func (p *PathToKey) isLeftmost() bool {
	for _, node := range p.InnerNodes {
		if node.Left != nil {
			return false
		}
	}
	return true
}

func (p *PathToKey) isRightmost() bool {
	for _, node := range p.InnerNodes {
		if node.Right != nil {
			return false
		}
	}
	return true
}

type KeyExistsProof struct {
	PathToKey `json:"path"`
	RootHash  data.Bytes `json:"root_hash"`
}

func (proof *KeyExistsProof) Verify(key []byte, value []byte, root []byte) error {
	if !bytes.Equal(proof.RootHash, root) {
		return errors.New("roots are not equal")
	}
	leafNode := IAVLProofLeafNode{KeyBytes: key, ValueBytes: value}
	return proof.PathToKey.verify(leafNode, root)
}

type KeyNotExistsProof struct {
	RootHash data.Bytes `json:"root_hash"`

	LeftPath *PathToKey        `json:"left_path"`
	LeftNode IAVLProofLeafNode `json:"left_node"`

	RightPath *PathToKey        `json:"right_path"`
	RightNode IAVLProofLeafNode `json:"right_node"`
}

func (p *KeyNotExistsProof) String() string {
	return fmt.Sprintf("KeyNotExistsProof\nroot=%x\nleft=%s%#v\nright=%s%#v\n", p.RootHash, p.LeftPath, p.LeftNode, p.RightPath, p.RightNode)
}

func (proof *KeyNotExistsProof) Verify(key []byte, root []byte) error {
	if !bytes.Equal(proof.RootHash, root) {
		return errors.New("roots do not match")
	}

	if proof.LeftPath == nil && proof.RightPath == nil {
		return errors.New("at least one path must exist")
	}
	if proof.LeftPath != nil {
		if err := proof.LeftPath.verify(proof.LeftNode, root); err != nil {
			return errors.New("failed to verify left path")
		}
		if bytes.Compare(proof.LeftNode.KeyBytes, key) != -1 {
			return errors.New("left node key must be lesser than supplied key")
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.New("failed to verify right path")
		}
		if bytes.Compare(proof.RightNode.KeyBytes, key) != 1 {
			return errors.New("left node key must be greater than supplied key")
		}
	}

	// Both paths exist, check that they are sequential.
	if proof.RightPath != nil && proof.LeftPath != nil {
		lns := proof.LeftPath.InnerNodes[:]
		rns := proof.RightPath.InnerNodes[:]

		for bytes.Compare(lns[len(lns)-1].Left, rns[len(rns)-1].Left) == 0 &&
			bytes.Compare(lns[len(lns)-1].Right, rns[len(rns)-1].Right) == 0 {

			lns = lns[:len(lns)-1]
			rns = rns[:len(rns)-1]
		}
		lpath := &PathToKey{InnerNodes: lns[:len(lns)-1]}
		rpath := &PathToKey{InnerNodes: rns[:len(lns)-1]}

		if !lpath.isRightmost() || !rpath.isLeftmost() {
			return errors.New("merkle paths are not adjacent")
		}
		return nil
	}

	// Only right path exists, check that node is at left boundary.
	if proof.LeftPath == nil {
		if !proof.RightPath.isLeftmost() {
			return errors.New("right path is only one but not leftmost")
		}
	}

	// Only left path exists, check that node is at right boundary.
	if proof.RightPath == nil {
		if !proof.LeftPath.isRightmost() {
			return errors.New("left path is only one but not rightmost")
		}
	}

	return nil
}

type KeyRangeExistsProof struct {
	RootHash   data.Bytes
	PathToKeys []*PathToKey
}

func (proof *KeyRangeExistsProof) Verify(key []byte, value []byte, root []byte) bool {
	return false
}

func (node *IAVLNode) constructKeyExistsProof(t *IAVLTree, key []byte, proof *KeyExistsProof) ([]byte, error) {
	if node.height == 0 {
		if bytes.Compare(node.key, key) == 0 {
			proof.LeafHash = node.hash
			return node.value, nil
		}
		return nil, errors.New("key does not exist")
	}

	if bytes.Compare(key, node.key) < 0 {
		if value, err := node.getLeftNode(t).constructKeyExistsProof(t, key, proof); err == nil {
			branch := IAVLProofInnerNode{
				Height: node.height,
				Size:   node.size,
				Left:   nil,
				Right:  node.getRightNode(t).hash,
			}
			proof.InnerNodes = append(proof.InnerNodes, branch)
			return value, nil
		}
		return nil, errors.New("key does not exist")
	}

	if value, err := node.getRightNode(t).constructKeyExistsProof(t, key, proof); err == nil {
		branch := IAVLProofInnerNode{
			Height: node.height,
			Size:   node.size,
			Left:   node.getLeftNode(t).hash,
			Right:  nil,
		}
		proof.InnerNodes = append(proof.InnerNodes, branch)
		return value, nil
	}
	return nil, errors.New("key does not exist")
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
	if idx <= t.Size()-1 {
		rkey, rval = t.GetByIndex(idx)
	}

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

func (t *IAVLTree) getWithKeyExistsProof(key []byte) (value []byte, proof *KeyExistsProof, err error) {
	if t.root == nil {
		return nil, nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof = &KeyExistsProof{
		RootHash: t.root.hash,
	}
	value, err = t.root.constructKeyExistsProof(t, key, proof)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not construct proof of existence")
	}
	return value, proof, nil
}

func (t *IAVLTree) keyNotExistsProof(key []byte) (*KeyNotExistsProof, error) {
	if t.root == nil {
		return nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof := &KeyNotExistsProof{
		RootHash: t.root.hash,
	}
	if err := t.root.constructKeyNotExistsProof(t, key, proof); err != nil {
		return nil, errors.Wrap(err, "could not construct proof of non-existence")
	}
	return proof, nil
}
