package iavl

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/tendermint/go-wire/data"
)

var (
	InvalidProofErr  = errors.New("invalid proof")
	InvalidPathErr   = errors.New("invalid path")
	InvalidInputsErr = errors.New("invalid inputs")
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
		return InvalidPathErr
	}
	hash := leafHash
	for _, branch := range p.InnerNodes {
		hash = branch.Hash(hash)
	}
	if !bytes.Equal(root, hash) {
		return InvalidPathErr
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
	return p == nil || len(p.InnerNodes) == 0
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

func (p *PathToKey) hasCommonRoot(p2 *PathToKey) bool {
	if p.isEmpty() || p2.isEmpty() {
		return false
	}
	leftEnd := p.InnerNodes[len(p.InnerNodes)-1]
	rightEnd := p2.InnerNodes[len(p2.InnerNodes)-1]

	return bytes.Equal(leftEnd.Left, rightEnd.Left) &&
		bytes.Equal(leftEnd.Right, rightEnd.Right)
}

func (p *PathToKey) isLeftAdjacentTo(p2 *PathToKey) bool {
	for p.hasCommonRoot(p2) {
		p, p2 = p.dropRoot(), p2.dropRoot()
	}
	p, p2 = p.dropRoot(), p2.dropRoot()

	return p.isRightmost() && p2.isLeftmost()
}

type KeyExistsProof struct {
	*PathToKey `json:"path"`
	RootHash   data.Bytes `json:"root_hash"`
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

type KeyFirstInRangeProof struct {
	RootHash  data.Bytes `json:"root_hash"`
	PathToKey *PathToKey `json:"path"`

	LeftPath *PathToKey        `json:"left_path"`
	LeftNode IAVLProofLeafNode `json:"left_node"`

	RightPath *PathToKey        `json:"right_path"`
	RightNode IAVLProofLeafNode `json:"right_node"`
}

func (proof *KeyFirstInRangeProof) String() string {
	return fmt.Sprintf("%#v", proof)
}

func (proof *KeyFirstInRangeProof) Verify(startKey, endKey, key, value []byte, root []byte) (err error) {
	if !bytes.Equal(proof.RootHash, root) {
		return errors.New("roots do not match")
	}

	leafNode := IAVLProofLeafNode{KeyBytes: key, ValueBytes: value}

	if proof.PathToKey != nil {
		if err := proof.PathToKey.verify(leafNode, root); err != nil {
			return errors.Wrap(err, "failed to verify inner path")
		}
		if bytes.Equal(key, startKey) {
			return
		}
		if leafNode.isGreaterThan(startKey) {
			if proof.PathToKey.isLeftmost() {
				return
			}
			if proof.LeftPath != nil && proof.LeftNode.isLesserThan(startKey) &&
				proof.LeftPath.isLeftAdjacentTo(proof.PathToKey) {
				return
			}
		}
	}

	if proof.LeftPath != nil {
		if err := proof.LeftPath.verify(proof.LeftNode, root); err != nil {
			return errors.Wrap(err, "failed to verify left path")
		}
		if !proof.LeftNode.isLesserThan(startKey) {
			return errors.New("left node must be lesser than start key")
		}
		if proof.RightPath == nil {
			if !proof.LeftPath.isRightmost() {
				return errors.New("left path is not rightmost")
			}
			return
		}
	}

	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.Wrap(err, "failed to verify right path")
		}
		if !proof.RightNode.isGreaterThan(endKey) {
			return errors.New("right node must be greater than end key")
		}
		if proof.LeftPath == nil {
			if !proof.RightPath.isLeftmost() {
				return errors.New("right path is not leftmost")
			}
			return
		}
	}

	if proof.LeftPath != nil && proof.RightPath != nil {
		if !proof.LeftPath.isLeftAdjacentTo(proof.RightPath) {
			return errors.New("left and right paths are not adjacent")
		}
		return
	}

	return InvalidProofErr
}

type KeyLastInRangeProof struct {
	RootHash  data.Bytes `json:"root_hash"`
	PathToKey *PathToKey `json:"path"`

	LeftPath *PathToKey        `json:"left_path"`
	LeftNode IAVLProofLeafNode `json:"left_node"`

	RightPath *PathToKey        `json:"right_path"`
	RightNode IAVLProofLeafNode `json:"right_node"`
}

func (proof *KeyLastInRangeProof) String() string {
	inner := ""
	if proof.PathToKey != nil {
		inner = fmt.Sprintf("PathToKey: \n\t%s", proof.PathToKey.String())
	}
	inner += fmt.Sprintf("LeftNode: %#v (%x)\n", proof.LeftNode, proof.LeftNode.Hash())
	inner += fmt.Sprintf("RightNode: %#v (%x)", proof.RightNode, proof.RightNode.Hash())
	return "&KeyLastRangeProof{\n\t" + inner + "\n}"
}

func (proof *KeyLastInRangeProof) Verify(startKey, endKey, key, value []byte, root []byte) (err error) {
	if !bytes.Equal(proof.RootHash, root) {
		return InvalidProofErr
	}
	if key != nil && (bytes.Compare(startKey, key) == 1 || bytes.Compare(key, endKey) == 1) {
		return InvalidInputsErr
	}

	if proof.LeftPath != nil {
		if err := proof.LeftPath.verify(proof.LeftNode, root); err != nil {
			return errors.Wrap(err, "failed to verify left path")
		}
		if !proof.LeftNode.isLesserThan(startKey) {
			return InvalidProofErr
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.Wrap(err, "failed to verify right path")
		}
		if !proof.RightNode.isGreaterThan(endKey) {
			return InvalidProofErr
		}
	}

	leafNode := IAVLProofLeafNode{KeyBytes: key, ValueBytes: value}

	if proof.PathToKey != nil {
		if err := proof.PathToKey.verify(leafNode, root); err != nil {
			return errors.Wrap(err, "failed to verify inner path")
		}
		if bytes.Equal(key, endKey) {
			return
		}
		if leafNode.isLesserThan(endKey) {
			if proof.PathToKey.isRightmost() {
				return
			}
			if proof.RightPath != nil &&
				proof.PathToKey.isLeftAdjacentTo(proof.RightPath) {
				return
			}
			if proof.LeftPath != nil &&
				proof.LeftPath.isLeftAdjacentTo(proof.PathToKey) {
				return
			}
		}
	} else if proof.LeftPath != nil && proof.RightPath != nil &&
		proof.LeftPath.isLeftAdjacentTo(proof.RightPath) {
		return
	} else if proof.RightPath != nil && proof.RightPath.isLeftmost() {
		return
	} else if proof.LeftPath != nil && proof.LeftPath.isRightmost() {
		return
	}
	return InvalidProofErr
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
		if !paths[i].isLeftAdjacentTo(paths[i+1]) {
			return errors.Errorf("paths #%d and #%d are not adjacent", i, i+1)
		}
	}
	return nil
}

// Verify that a range proof is valid.
func (proof *KeyRangeProof) Verify(
	startKey, endKey []byte, keys, values [][]byte, root []byte,
) error {
	if len(proof.PathToKeys) != len(keys) || len(values) != len(keys) {
		return errors.New("wrong number of keys or values for proof")
	}
	if len(proof.PathToKeys) == 0 && proof.LeftPath.isEmpty() && proof.RightPath.isEmpty() {
		return errors.New("proof is incomplete")
	}

	// If startKey > endKey, reverse the keys and values, since our proofs are
	// always in ascending order.
	if bytes.Compare(startKey, endKey) == 1 {
		startKey, endKey = endKey, startKey

		ks := make([][]byte, len(keys))
		vs := make([][]byte, len(keys))
		for i, _ := range keys {
			ks[len(ks)-1-i] = keys[i]
			vs[len(vs)-1-i] = values[i]
		}
		keys, values = ks, vs
	}

	// Verify that all paths are adjacent to one another.
	if err := proof.verifyPathAdjacency(); err != nil {
		return err
	}

	firstKey, lastKey := startKey, endKey
	if len(keys) > 0 {
		firstKey, lastKey = keys[0], keys[len(keys)-1]
	}

	if proof.LeftPath != nil {
		if err := proof.LeftPath.verify(proof.LeftNode, root); err != nil {
			return errors.Wrap(err, "failed to verify left path")
		}
		if bytes.Compare(proof.LeftNode.KeyBytes, firstKey) != -1 {
			return errors.New("left node must be lesser than start key")
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.Wrap(err, "failed to verify right path")
		}
		if bytes.Compare(proof.RightNode.KeyBytes, lastKey) == -1 {
			return errors.New("right node must be greater or equal than end key")
		}
	}

	// If proof.PathToKeys is empty, it means we have an empty range. This range
	// can be between keys, or outside of the range of existing keys.
	if len(proof.PathToKeys) == 0 {
		// Range is outisde and to the left of existing keys.
		if proof.LeftPath == nil && proof.RightPath.isLeftmost() {
			return nil
		}
		// Range is outisde and to the right of existing keys.
		if proof.RightPath == nil && proof.LeftPath.isRightmost() {
			return nil
		}
		// Range is between two existing keys.
		if proof.LeftPath != nil && proof.RightPath != nil {
			return nil
		}
		return errors.New("invalid proof of empty range")
	}

	if !bytes.Equal(startKey, keys[0]) && proof.LeftPath == nil {
		if !proof.PathToKeys[0].isLeftmost() {
			return errors.New("left path is nil and first inner path is not leftmost")
		}
	}
	if !bytes.Equal(endKey, keys[len(keys)-1]) && proof.RightPath == nil {
		if !proof.PathToKeys[len(proof.PathToKeys)-1].isRightmost() {
			return errors.New("right path is nil and last inner path is not rightmost")
		}
	}

	// If we've reached this point, it means our range isn't empty, and we have
	// a list of keys.
	for i, path := range proof.PathToKeys {
		leafNode := IAVLProofLeafNode{KeyBytes: keys[i], ValueBytes: values[i]}
		if err := path.verify(leafNode, root); err != nil {
			return errors.Wrap(err, "failed to verify inner path")
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

func (node *IAVLNode) constructKeyRangeProof(
	t *IAVLTree, keyStart, keyEnd []byte, limit int, rangeProof *KeyRangeProof,
) (
	keys [][]byte, values [][]byte, err error,
) {
	ascending := bytes.Compare(keyStart, keyEnd) == -1
	if !ascending {
		keyStart, keyEnd = keyEnd, keyStart
	}

	limited := t.IterateRangeInclusive(keyStart, keyEnd, ascending, func(k, v []byte) bool {
		keys = append(keys, k)
		values = append(values, v)
		return len(keys) == limit
	})

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

	first, last := 0, len(keys)-1
	if !ascending {
		first, last = last, first
	}

	// So far, we've created proofs of the keys which are within the provided range.
	// Next, we need to create a proof that we haven't omitted any keys to the left
	// or right of that range. This is relevant in two scenarios:
	//
	// 1. There are no keys in the range. In this case, include a proof of the key
	//    to the left and right of that empty range.
	// 2. The start or end key do not match the start and end of the keys returned.
	//    In this case, include proofs of the keys immediately outside of those returned.
	//
	if len(keys) == 0 || !bytes.Equal(keys[first], keyStart) {
		if limited {
			keyStart = keys[first]
		}
		// Find index of first key to the left, and include proof if it isn't the
		// leftmost key.
		if idx, _, _ := t.Get(keyStart); idx > 0 {
			lkey, lval := t.GetByIndex(idx - 1)
			path, _, _ := node.pathToKey(t, lkey)
			rangeProof.LeftPath = path
			rangeProof.LeftNode = IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval}
		}
	}
	if len(keys) == 0 || !bytes.Equal(keys[last], keyEnd) {
		if limited {
			keyEnd = keys[last]
		}
		// Find index of first key to the right, and include proof if it isn't the
		// rightmost key.
		idx, _, _ := t.Get(keyEnd)
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
	keys, vals, rangeProof, err := t.getRangeWithProof(keyStart, keyEnd, 1)
	if err != nil {
		return
	}

	proof = &KeyFirstInRangeProof{
		RootHash: rangeProof.RootHash,

		LeftPath: rangeProof.LeftPath,
		LeftNode: rangeProof.LeftNode,

		RightPath: rangeProof.RightPath,
		RightNode: rangeProof.RightNode,
	}

	if len(keys) == 0 {
		return
	}
	proof.PathToKey = rangeProof.PathToKeys[0]

	return keys[0], vals[0], proof, nil
}

func (t *IAVLTree) getLastInRangeWithProof(keyStart, keyEnd []byte) (
	key, value []byte, proof *KeyLastInRangeProof, err error,
) {
	if t.root == nil {
		return nil, nil, nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.
	proof = &KeyLastInRangeProof{
		RootHash: t.root.hash,
	}
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
