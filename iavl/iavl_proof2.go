package iavl

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"github.com/tendermint/go-wire/data"
)

// PathToKey represents an inner path to a leaf node.
// Note that the nodes are ordered such that the last one is closest
// to the root of the tree.
type PathToKey struct {
	LeafHash   data.Bytes           `json:"leaf_hash"`
	InnerNodes []IAVLProofInnerNode `json:"inner_nodes"`
}

func (p *PathToKey) String() string {
	str := ""
	for i := len(p.InnerNodes) - 1; i >= 0; i-- {
		str += p.InnerNodes[i].String() + "\n"
	}
	str += fmt.Sprintf("hash(leaf)=%s\n", p.LeafHash)
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
		if len(node.Left) > 0 {
			return false
		}
	}
	return true
}

func (p *PathToKey) isRightmost() bool {
	for _, node := range p.InnerNodes {
		if len(node.Right) > 0 {
			return false
		}
	}
	return true
}

func (p *PathToKey) pop() *PathToKey {
	return &PathToKey{
		LeafHash:   p.LeafHash,
		InnerNodes: p.InnerNodes[:len(p.InnerNodes)-1],
	}
}

func (left *PathToKey) hasCommonAncestor(right *PathToKey) bool {
	leftEnd := left.InnerNodes[len(left.InnerNodes)-1]
	rightEnd := right.InnerNodes[len(right.InnerNodes)-1]

	return bytes.Equal(leftEnd.Left, rightEnd.Left) &&
		bytes.Equal(leftEnd.Right, rightEnd.Right)
}

func (left *PathToKey) isAdjacentTo(right *PathToKey) bool {
	for left.hasCommonAncestor(right) {
		left, right = left.pop(), right.pop()
	}
	left, right = left.pop(), right.pop()

	return left.isRightmost() && right.isLeftmost()
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
	return fmt.Sprintf("KeyNotExistsProof\nroot=%s\nleft=%s%#v\nright=%s%#v\n", p.RootHash, p.LeftPath, p.LeftNode, p.RightPath, p.RightNode)
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
			return errors.New("right node key must be greater than supplied key")
		}
	}

	// Both paths exist, check that they are sequential.
	if proof.RightPath != nil && proof.LeftPath != nil {
		if !proof.LeftPath.isAdjacentTo(proof.RightPath) {
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

type KeyRangeProof struct {
	RootHash   data.Bytes   `json:"root_hash"`
	PathToKeys []*PathToKey `json:"paths"`

	LeftPath *PathToKey        `json:"left_path"`
	LeftNode IAVLProofLeafNode `json:"left_node"`

	RightPath *PathToKey        `json:"right_path"`
	RightNode IAVLProofLeafNode `json:"right_node"`
}

// Verify that a range proof is valid.
func (proof *KeyRangeProof) Verify(
	startKey, endKey []byte, keys, values [][]byte, root []byte,
) error {
	// TODO: Check that range is what we asked for.

	// There are two things we want to verify here:
	//
	// 1. That the keys and values do indeed exist.
	// 2. That the set of keys represent *all* keys in the range. There are no
	//    keys that have been ommitted or are missing.
	//
	for i, path := range proof.PathToKeys {
		leafNode := IAVLProofLeafNode{KeyBytes: keys[i], ValueBytes: values[i]}
		err := path.verify(leafNode, root)
		if err != nil {
			return err
		}

		// TODO: We have to verify the beginning of the range.

		left := proof.PathToKeys[i]
		if i >= len(proof.PathToKeys)-1 {
			// We have an odd number of paths. We need to verify that the path
			// has nothing to its right that should be included in the range.
			// TODO
			break
		}
		right := proof.PathToKeys[i+1]

		// If the keys are descending, we have to check the other way around.
		if bytes.Compare(keys[i], keys[i+1]) == 1 {
			left, right = right, left
		}

		if !left.isAdjacentTo(right) {
			return errors.Errorf("paths %d and %d are not adjacent", i, i+1)
		}
	}

	// If our start or end key are outside of the range of keys returned, we need
	// to verify that no keys were ommitted in the result set. The additional
	// left and right paths in the proof are for that purpose: they are paths
	// to keys outside of the range of keys returned, yet adjacent to it, proving
	// that nothing lies in between.
	if !bytes.Equal(startKey, keys[0]) {
		if bytes.Compare(proof.LeftNode.KeyBytes, startKey) != -1 {
			// TODO: Error
		}
		if !proof.LeftPath.isAdjacentTo(proof.PathToKeys[0]) {
			// TODO: Error
		}
	}
	if !bytes.Equal(endKey, keys[len(keys)-1]) {
		if bytes.Compare(proof.RightNode.KeyBytes, endKey) != 1 {
			// TODO: Error
		}
		if !proof.RightPath.isAdjacentTo(proof.PathToKeys[len(proof.PathToKeys)-1]) {
			// TODO: Error
		}
	}
	return nil
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

func (node *IAVLNode) constructKeyRangeProof(t *IAVLTree, keyStart, keyEnd []byte, limit int, rangeProof *KeyRangeProof) ([][]byte, [][]byte, error) {
	keys := [][]byte{}
	values := [][]byte{}
	ascending := bytes.Compare(keyStart, keyEnd) == -1
	if !ascending {
		keyStart, keyEnd = keyEnd, keyStart
	}
	t.IterateRange(keyStart, keyEnd, ascending, func(k, v []byte) bool {
		keys = append(keys, k)
		values = append(values, v)
		keyProof := &KeyExistsProof{
			RootHash: t.root.hash,
		}
		node.constructKeyExistsProof(t, k, keyProof)
		rangeProof.PathToKeys = append(rangeProof.PathToKeys, &keyProof.PathToKey)
		return len(keys) == limit
	})

	// TODO: If range doesn't hit tree boundaries, include proofs of keys that are
	// greater than the range.

	return keys, values, nil
}

func (node *IAVLNode) constructKeyNotExistsProof(t *IAVLTree, key []byte, proof *KeyNotExistsProof) error {
	// Get the index of the first key greater than the requested key, if the key doesn't exist.
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

func (t *IAVLTree) getWithKeyRangeProof(keyStart, keyEnd []byte, limit int) (
	keys, values [][]byte, proof *KeyRangeProof, err error,
) {
	if t.root == nil {
		return nil, nil, nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.

	proof = &KeyRangeProof{
		RootHash: t.root.hash,
	}
	keys, values, err = t.root.constructKeyRangeProof(t, keyStart, keyEnd, limit, proof)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "could not construct proof of range")
	}
	return keys, values, proof, nil
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
