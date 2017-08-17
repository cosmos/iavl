package iavl

import (
	"bytes"
	"fmt"
	"strings"

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
		return errors.Errorf("leaf hashes do not match")
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

func (p *PathToKey) isEmpty() bool {
	return len(p.InnerNodes) == 0
}

func (p *PathToKey) dropRoot() *PathToKey {
	if p.isEmpty() {
		return p
	}
	return &PathToKey{
		LeafHash:   p.LeafHash,
		InnerNodes: p.InnerNodes[:len(p.InnerNodes)-1],
	}
}

func (left *PathToKey) hasCommonRoot(right *PathToKey) bool {
	if left.isEmpty() || right.isEmpty() {
		return false
	}
	leftEnd := left.InnerNodes[len(left.InnerNodes)-1]
	rightEnd := right.InnerNodes[len(right.InnerNodes)-1]

	return bytes.Equal(leftEnd.Left, rightEnd.Left) &&
		bytes.Equal(leftEnd.Right, rightEnd.Right)
}

func (left *PathToKey) isAdjacentTo(right *PathToKey) bool {
	for left.hasCommonRoot(right) {
		left, right = left.dropRoot(), right.dropRoot()
	}
	left, right = left.dropRoot(), right.dropRoot()

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
		if bytes.Compare(proof.LeftNode.KeyBytes, key) != -1 {
			return errors.New("left node key must be lesser than supplied key")
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.New("failed to verify right path")
		}
		if bytes.Compare(key, proof.RightNode.KeyBytes) != -1 {
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

// KeyRangeProof is proof that a range of keys does or does not exist.
type KeyRangeProof struct {
	RootHash   data.Bytes   `json:"root_hash"`
	PathToKeys []*PathToKey `json:"paths"`

	LeftPath *PathToKey        `json:"left_path"`
	LeftNode IAVLProofLeafNode `json:"left_node"`

	RightPath *PathToKey        `json:"right_path"`
	RightNode IAVLProofLeafNode `json:"right_node"`
}

func (proof *KeyRangeProof) String() string {
	paths := []string{}
	for _, p := range proof.PathToKeys {
		paths = append(paths, "\t"+p.String())
	}
	inner := fmt.Sprintf("PathToKeys: \n\t%s", strings.Join(paths, ",\n"))
	inner += fmt.Sprintf("LeftNode: %#v (%x)\n", proof.LeftNode, proof.LeftNode.Hash())
	inner += fmt.Sprintf("RightNode: %#v (%x)", proof.RightNode, proof.RightNode.Hash())
	return "&KeyRangeProof{\n\t" + inner + "\n}"
}

// Returns a list of all paths, in order.
func (proof *KeyRangeProof) paths() []*PathToKey {
	paths := proof.PathToKeys[:]
	if proof.LeftPath != nil {
		paths = append([]*PathToKey{proof.LeftPath}, paths...)
	}
	if proof.RightPath != nil {
		paths = append(paths, proof.RightPath)
	}
	return paths
}

// Checks that all paths are adjacent to one another, with no gaps.
func (proof *KeyRangeProof) verifyPathAdjacency() error {
	paths := proof.paths()
	for i := 0; i < len(paths)-1; i++ {
		// Always check from left to right, since paths are always in ascending order.
		if !paths[i].isAdjacentTo(paths[i+1]) {
			return errors.Errorf("paths #%d and #%d are not adjacent", i, i+1)
		}
	}
	return nil
}

// Verify that a range proof is valid.
func (proof *KeyRangeProof) Verify(
	startKey, endKey []byte, limit int, keys, values [][]byte, root []byte,
) error {
	if len(proof.PathToKeys) != len(keys) || len(values) != len(keys) {
		return errors.New("wrong number of keys or values for proof")
	}

	// If we've used a limit, our end key can be much greater than the right
	// node key, ex: with the query start=0, end=FF, limit=10, the returned
	// key range might be from 12 to D4, and the right node key will be the
	// one following D4, say DF. In this case, we set our start and end keys
	// to the key range returned.
	if len(keys) == limit && limit > 0 {
		endKey = keys[len(keys)-1]
		startKey = keys[0]
	}

	// If startKey > endKey, reverse the keys and values, since our proofs are
	// always in ascending order.
	if bytes.Compare(startKey, endKey) == 1 {
		startKey, endKey = endKey, startKey

		ks, vs := [][]byte{}, [][]byte{}
		for i := len(keys) - 1; i >= 0; i-- {
			ks = append(ks, keys[i])
			vs = append(vs, values[i])
		}
		keys, values = ks, vs
	}

	if proof.LeftPath != nil {
		if err := proof.LeftPath.verify(proof.LeftNode, root); err != nil {
			return errors.Wrap(err, "failed to verify left path")
		}
		if bytes.Compare(proof.LeftNode.KeyBytes, startKey) != -1 {
			return errors.New("left node must be lesser than start key")
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.Wrap(err, "failed to verify right path")
		}
		if bytes.Compare(endKey, proof.RightNode.KeyBytes) != -1 {
			return errors.New("right node must be greater than end key")
		}
	}

	// Verify that all paths are adjacent to one another.
	if err := proof.verifyPathAdjacency(); err != nil {
		return err
	}

	// If proof.PathToKeys is empty, it means we have an empty range. This range
	// can be between keys, or outside of the range of existing keys.
	if len(proof.PathToKeys) == 0 {
		if proof.LeftPath == nil && proof.RightPath == nil {
			return errors.New("proof is incomplete")
		}
		if proof.LeftPath == nil && proof.RightPath != nil {
			// Range is outisde and to the left of existing keys.
			if !proof.RightPath.isLeftmost() {
				return errors.New("right path is not leftmost")
			}
		} else if proof.RightPath == nil && proof.LeftPath != nil {
			// Range is outisde and to the right of existing keys.
			if !proof.LeftPath.isRightmost() {
				return errors.New("left path is not rightmost")
			}
		}
	} else {
		// If we've reached this point, it means our range isn't empty, and we have
		// a list of keys.
		for i, path := range proof.PathToKeys {
			leafNode := IAVLProofLeafNode{KeyBytes: keys[i], ValueBytes: values[i]}
			if err := path.verify(leafNode, root); err != nil {
				return errors.Wrap(err, "failed to verify inner path")
			}
		}

		// Our start key is lesser than the first key in the tree.
		// Hence, the first key returned must have nothing to its left.
		if proof.LeftPath == nil && !bytes.Equal(startKey, keys[0]) {
			if !proof.PathToKeys[0].isLeftmost() {
				return errors.New("left path is nil and first inner path is not leftmost")
			}
		}
		// Our end key is greater than the last key in the tree.
		// Hence, the last key returned must have nothing to its right.
		if proof.RightPath == nil && !bytes.Equal(endKey, keys[len(keys)-1]) {
			if !proof.PathToKeys[len(proof.PathToKeys)-1].isRightmost() {
				return errors.New("right path is nil and last inner path is not rightmost")
			}
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

func (node *IAVLNode) constructKeyRangeProof(t *IAVLTree, keyStart, keyEnd []byte, limit int, rangeProof *KeyRangeProof) ([][]byte, [][]byte, error) {
	keys := [][]byte{}
	values := [][]byte{}
	ascending := bytes.Compare(keyStart, keyEnd) == -1
	if !ascending {
		keyStart, keyEnd = keyEnd, keyStart
	}

	limited := t.IterateRangeInclusive(keyStart, keyEnd, ascending, func(k, v []byte) bool {
		keys = append(keys, k)
		values = append(values, v)
		return len(keys) == limit
	})

	first, last := 0, len(keys)-1
	if !ascending {
		first, last = last, first
	}

	// Construct the paths such that they are always in ascending order.
	rangeProof.PathToKeys = make([]*PathToKey, len(keys))
	for i, k := range keys {
		path, _, _ := node.pathToKey(t, k)
		if ascending {
			rangeProof.PathToKeys[i] = path
		} else {
			rangeProof.PathToKeys[len(keys)-i-1] = path
		}
	}

	if limited {
		keyStart, keyEnd = keys[first], keys[last]
	}

	if limited || len(keys) == 0 || !bytes.Equal(keys[first], keyStart) {
		if idx, _, _ := t.Get(keyStart); idx > 0 {
			lkey, lval := t.GetByIndex(idx - 1)
			path, _, _ := node.pathToKey(t, lkey)
			rangeProof.LeftPath = path
			rangeProof.LeftNode = IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval}
		}
	}

	if limited || len(keys) == 0 || !bytes.Equal(keys[last], keyEnd) {
		idx, _, _ := t.Get(keyEnd)
		if limited {
			idx++
		}
		if idx <= t.Size()-1 {
			rkey, rval := t.GetByIndex(idx)
			path, _, _ := node.pathToKey(t, rkey)
			rangeProof.RightPath = path
			rangeProof.RightNode = IAVLProofLeafNode{KeyBytes: rkey, ValueBytes: rval}
		}
	}

	return keys, values, nil
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
		PathToKey: *path,
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
