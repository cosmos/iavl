package iavl

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"

	"github.com/tendermint/go-wire/data"
)

var (
	ErrInvalidProof  = errors.New("invalid proof")
	ErrInvalidPath   = errors.New("invalid path")
	ErrInvalidInputs = errors.New("invalid inputs")
)

type KeyProof interface {
	Verify(key, value, root []byte) error
}

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

	Left  *PathWithNode `json:"left"`
	Right *PathWithNode `json:"right"`
}

func (p *KeyAbsentProof) String() string {
	return fmt.Sprintf("KeyAbsentProof\nroot=%s\nleft=%s%#v\nright=%s%#v\n", p.RootHash, p.Left.Path, p.Left.Node, p.Right.Path, p.Right.Node)
}

func (proof *KeyAbsentProof) Verify(key, value []byte, root []byte) error {
	if !bytes.Equal(proof.RootHash, root) {
		return errors.New("roots do not match")
	}

	if value != nil {
		return ErrInvalidInputs
	}
	if proof.Left == nil && proof.Right == nil {
		return errors.New("at least one path must exist")
	}
	if err := verifyPaths(proof.Left, proof.Right, key, key, root); err != nil {
		return err
	}

	if proof.Left == nil && proof.Right.Path.isLeftmost() {
		return nil
	} else if proof.Right == nil && proof.Left.Path.isRightmost() {
		return nil
	} else if proof.Left.Path.isLeftAdjacentTo(proof.Right.Path) {
		return nil
	}

	return ErrInvalidProof
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

func (t *IAVLTree) constructKeyAbsentProof(key []byte, proof *KeyAbsentProof) error {
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
		path, _, _ := t.root.pathToKey(t, lkey)
		proof.Left = &PathWithNode{
			Path: path,
			Node: IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval},
		}
	}
	if rkey != nil {
		path, _, _ := t.root.pathToKey(t, rkey)
		proof.Right = &PathWithNode{
			Path: path,
			Node: IAVLProofLeafNode{KeyBytes: rkey, ValueBytes: rval},
		}
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

func (t *IAVLTree) keyAbsentProof(key []byte) (*KeyAbsentProof, error) {
	if t.root == nil {
		return nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof := &KeyAbsentProof{
		RootHash: t.root.hash,
	}
	if err := t.constructKeyAbsentProof(key, proof); err != nil {
		return nil, errors.Wrap(err, "could not construct proof of non-existence")
	}
	return proof, nil
}
