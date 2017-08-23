package iavl

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"

	"github.com/tendermint/go-wire/data"
)

var (
	InvalidProofErr  = errors.New("invalid proof")
	InvalidPathErr   = errors.New("invalid path")
	InvalidInputsErr = errors.New("invalid inputs")
)

type KeyExistsProof struct {
	RootHash   data.Bytes `json:"root_hash"`
	*PathToKey `json:"path"`
}

func (proof *KeyExistsProof) Verify(key []byte, value []byte, root []byte) error {
	if !bytes.Equal(proof.RootHash, root) {
		return errors.New("roots are not equal")
	}
	leafNode := IAVLProofLeafNode{KeyBytes: key, ValueBytes: value}
	return proof.PathToKey.verify(leafNode, root)
}

type KeyAbsentProof struct {
	RootHash data.Bytes `json:"root_hash"`

	LeftPath *PathToKey        `json:"left_path"`
	LeftNode IAVLProofLeafNode `json:"left_node"`

	RightPath *PathToKey        `json:"right_path"`
	RightNode IAVLProofLeafNode `json:"right_node"`
}

func (p *KeyAbsentProof) String() string {
	return fmt.Sprintf("KeyAbsentProof\nroot=%s\nleft=%s%#v\nright=%s%#v\n", p.RootHash, p.LeftPath, p.LeftNode, p.RightPath, p.RightNode)
}

func (proof *KeyAbsentProof) Verify(key []byte, root []byte) error {
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
		if !proof.LeftNode.isLesserThan(key) {
			return errors.New("left node key must be lesser than supplied key")
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.New("failed to verify right path")
		}
		if !proof.RightNode.isGreaterThan(key) {
			return errors.New("right node key must be greater than supplied key")
		}
	}

	// Both paths exist, check that they are sequential.
	if proof.RightPath != nil && proof.LeftPath != nil {
		if !proof.LeftPath.isLeftAdjacentTo(proof.RightPath) {
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

func (node *IAVLNode) pathToKey(t *IAVLTree, key []byte) (*PathToKey, []byte, error) {
	path := &PathToKey{}
	val, err := node._pathToKey(t, key, path)
	return path, val, err
}
func (node *IAVLNode) _pathToKey(t *IAVLTree, key []byte, path *PathToKey) ([]byte, error) {
	if node.height == 0 {
		if bytes.Compare(node.key, key) == 0 {
			path.LeafHash = node.hash
			return node.value, nil
		}
		return nil, errors.New("key does not exist")
	}

	if bytes.Compare(key, node.key) < 0 {
		if value, err := node.getLeftNode(t)._pathToKey(t, key, path); err == nil {
			branch := IAVLProofInnerNode{
				Height: node.height,
				Size:   node.size,
				Left:   nil,
				Right:  node.getRightNode(t).hash,
			}
			path.InnerNodes = append(path.InnerNodes, branch)
			return value, nil
		}
		return nil, errors.New("key does not exist")
	}

	if value, err := node.getRightNode(t)._pathToKey(t, key, path); err == nil {
		branch := IAVLProofInnerNode{
			Height: node.height,
			Size:   node.size,
			Left:   node.getLeftNode(t).hash,
			Right:  nil,
		}
		path.InnerNodes = append(path.InnerNodes, branch)
		return value, nil
	}
	return nil, errors.New("key does not exist")
}

func (node *IAVLNode) constructKeyAbsentProof(t *IAVLTree, key []byte, proof *KeyAbsentProof) error {
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
		path, _, _ := node.pathToKey(t, lkey)
		proof.LeftPath = path
		proof.LeftNode = IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval}
	}
	if rkey != nil {
		path, _, _ := node.pathToKey(t, rkey)
		proof.RightPath = path
		proof.RightNode = IAVLProofLeafNode{KeyBytes: rkey, ValueBytes: rval}
	}

	return nil
}

func (t *IAVLTree) getWithProof(key []byte) (value []byte, proof *KeyExistsProof, err error) {
	if t.root == nil {
		return nil, nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.

	path, value, err := t.root.pathToKey(t, key)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not construct path to key")
	}

	proof = &KeyExistsProof{
		RootHash:  t.root.hash,
		PathToKey: path,
	}
	return value, proof, nil
}

func (t *IAVLTree) getRangeWithProof(keyStart, keyEnd []byte, limit int) (
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

func (t *IAVLTree) getFirstInRangeWithProof(keyStart, keyEnd []byte) (
	key, value []byte, proof *KeyFirstInRangeProof, err error,
) {
	if t.root == nil {
		return nil, nil, nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof = &KeyFirstInRangeProof{}
	proof.RootHash = t.root.hash

	// Get the first value in the range.
	t.IterateRangeInclusive(keyStart, keyEnd, true, func(k, v []byte) bool {
		key, value = k, v
		return true
	})

	if len(key) > 0 {
		proof.PathToKey, _, _ = t.root.pathToKey(t, key)
	}

	if !bytes.Equal(key, keyStart) {
		if idx, _, _ := t.Get(keyStart); idx-1 >= 0 && idx-1 <= t.Size()-1 {
			k, v := t.GetByIndex(idx - 1)
			proof.LeftPath, _, _ = t.root.pathToKey(t, k)
			proof.LeftNode = IAVLProofLeafNode{k, v}
		}
	}

	if !bytes.Equal(key, keyEnd) {
		if idx, _, exists := t.Get(keyEnd); idx <= t.Size()-1 && !exists {
			k, v := t.GetByIndex(idx)
			proof.RightPath, _, _ = t.root.pathToKey(t, k)
			proof.RightNode = IAVLProofLeafNode{KeyBytes: k, ValueBytes: v}
		}
	}

	return key, value, proof, nil
}

func (t *IAVLTree) getLastInRangeWithProof(keyStart, keyEnd []byte) (
	key, value []byte, proof *KeyLastInRangeProof, err error,
) {
	if t.root == nil {
		return nil, nil, nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof = &KeyLastInRangeProof{}
	proof.RootHash = t.root.hash

	key, value, err = t.root.constructLastInRangeProof(t, keyStart, keyEnd, proof)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "could not construct last-in-range proof")
	}
	return key, value, proof, nil
}

func (node *IAVLNode) constructLastInRangeProof(t *IAVLTree, keyStart, keyEnd []byte, proof *KeyLastInRangeProof) (key, value []byte, err error) {
	// Get the last value in the range.
	t.IterateRangeInclusive(keyStart, keyEnd, false, func(k, v []byte) bool {
		key, value = k, v
		return true
	})

	if len(key) > 0 {
		proof.PathToKey, _, _ = t.root.pathToKey(t, key)
	}

	if !bytes.Equal(key, keyEnd) {
		if idx, _, _ := t.Get(keyEnd); idx <= t.Size()-1 {
			k, v := t.GetByIndex(idx)
			proof.RightPath, _, _ = node.pathToKey(t, k)
			proof.RightNode = IAVLProofLeafNode{KeyBytes: k, ValueBytes: v}
		}
	}

	if !bytes.Equal(key, keyStart) {
		if idx, _, _ := t.Get(keyStart); idx-1 >= 0 && idx-1 <= t.Size()-1 {
			k, v := t.GetByIndex(idx - 1)
			proof.LeftPath, _, _ = node.pathToKey(t, k)
			proof.LeftNode = IAVLProofLeafNode{k, v}
		}
	}

	return key, value, nil
}

func (t *IAVLTree) keyAbsentProof(key []byte) (*KeyAbsentProof, error) {
	if t.root == nil {
		return nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof := &KeyAbsentProof{
		RootHash: t.root.hash,
	}
	if err := t.root.constructKeyAbsentProof(t, key, proof); err != nil {
		return nil, errors.Wrap(err, "could not construct proof of non-existence")
	}
	return proof, nil
}
