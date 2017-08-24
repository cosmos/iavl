package iavl

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/tendermint/go-wire/data"
)

// KeyFirstInRangeProof is a proof that a given key is the first in a given range.
type KeyFirstInRangeProof struct {
	KeyExistsProof `json:"key_proof"`

	Left  *PathWithNode `json:"left"`
	Right *PathWithNode `json:"right"`
}

// String returns a string representation of the proof.
func (proof *KeyFirstInRangeProof) String() string {
	return fmt.Sprintf("%#v", proof)
}

// Verify that the first in range proof is valid.
func (proof *KeyFirstInRangeProof) Verify(startKey, endKey, key, value []byte, root []byte) (err error) {
	if key != nil && (bytes.Compare(key, startKey) == -1 || bytes.Compare(key, endKey) == 1) {
		return ErrInvalidInputs
	}
	if proof.Left == nil && proof.Right == nil && proof.PathToKey == nil {
		return ErrInvalidProof
	}
	if err := verifyPaths(proof.Left, proof.Right, startKey, endKey, root); err != nil {
		return err
	}

	if proof.PathToKey != nil {
		if err := proof.KeyExistsProof.Verify(key, value, root); err != nil {
			return errors.Wrap(err, "failed to verify key exists proof")
		}
		// If the key returned is equal to our start key, and we've verified
		// that it exists, there's nothing else to check.
		if bytes.Equal(key, startKey) {
			return
		}
		// If the key returned is the smallest in the tree, then it must be
		// the smallest in the given range too.
		if proof.PathToKey.isLeftmost() {
			return
		}
		// The start key is in between the left path and the key returned,
		// and the paths are adjacent. Therefore there is nothing between
		// the key returned and the start key.
		if proof.Left != nil && proof.Left.Path.isLeftAdjacentTo(proof.PathToKey) {
			return
		}
	} else if proof.Left != nil && proof.Left.Path.isRightmost() {
		// No key found. Range starts outside of the right boundary.
		return
	} else if proof.Right != nil && proof.Right.Path.isLeftmost() {
		// No key found. Range ends outside of the left boundary.
		return
	} else if proof.Left.Path.isLeftAdjacentTo(proof.Right.Path) {
		// No key found. Range is between two existing keys.
		return
	}

	return ErrInvalidProof
}

///////////////////////////////////////////////////////////////////////////////

// KeyLastInRangeProof is a proof that a given key is the last in a given range.
type KeyLastInRangeProof struct {
	KeyExistsProof `json:"key_proof"`

	Left  *PathWithNode `json:"left"`
	Right *PathWithNode `json:"right"`
}

// String returns a string representation of the proof.
func (proof *KeyLastInRangeProof) String() string {
	inner := ""
	if proof.PathToKey != nil {
		inner = fmt.Sprintf("PathToKey: \n\t%s", proof.PathToKey.String())
	}
	if proof.Left != nil {
		inner += fmt.Sprintf("Left: %#v (%x)\n", proof.Left.Node, proof.Left.Node.Hash())
	} else {
		inner += "Left: <nil>\n"
	}
	if proof.Right != nil {
		inner += fmt.Sprintf("RightNode: %#v (%x)", proof.Right.Node, proof.Right.Node.Hash())
	} else {
		inner += "Right: <nil>\n"
	}
	return "&KeyLastRangeProof{\n\t" + inner + "\n}"
}

// Verify that the last in range proof is valid.
func (proof *KeyLastInRangeProof) Verify(startKey, endKey, key, value []byte, root []byte) (err error) {
	if key != nil && (bytes.Compare(key, startKey) == -1 || bytes.Compare(key, endKey) == 1) {
		return ErrInvalidInputs
	}
	if proof.Left == nil && proof.Right == nil && proof.PathToKey == nil {
		return ErrInvalidProof
	}
	if err := verifyPaths(proof.Left, proof.Right, startKey, endKey, root); err != nil {
		return err
	}

	if proof.PathToKey != nil {
		if err := proof.KeyExistsProof.Verify(key, value, root); err != nil {
			return err
		}
		if bytes.Equal(key, endKey) {
			return
		}
		if proof.PathToKey.isRightmost() {
			return
		}
		if proof.Right != nil &&
			proof.PathToKey.isLeftAdjacentTo(proof.Right.Path) {
			return
		}
	} else if proof.Right != nil && proof.Right.Path.isLeftmost() {
		return
	} else if proof.Left != nil && proof.Left.Path.isRightmost() {
		return
	} else if proof.Left.Path.isLeftAdjacentTo(proof.Right.Path) {
		return
	}
	return ErrInvalidProof
}

///////////////////////////////////////////////////////////////////////////////

// KeyRangeProof is proof that a range of keys does or does not exist.
type KeyRangeProof struct {
	RootHash   data.Bytes   `json:"root_hash"`
	PathToKeys []*PathToKey `json:"paths"`

	Left  *PathWithNode `json:"left"`
	Right *PathWithNode `json:"right"`
}

// Verify that a range proof is valid.
func (proof *KeyRangeProof) Verify(
	startKey, endKey []byte, limit int, keys, values [][]byte, root []byte,
) error {
	if len(proof.PathToKeys) != len(keys) || len(values) != len(keys) {
		return errors.New("wrong number of keys or values for proof")
	}
	if len(proof.PathToKeys) == 0 && proof.Left == nil && proof.Right == nil {
		return errors.New("proof is incomplete")
	}

	// If startKey > endKey, reverse the keys and values, since our proofs are
	// always in ascending order.
	startKey, endKey, keys, values = reverseIfDescending(startKey, endKey, keys, values)

	// Verify that all paths are adjacent to one another.
	if err := proof.verifyPathAdjacency(); err != nil {
		return err
	}

	firstKey, lastKey := startKey, endKey
	if len(keys) > 0 {
		firstKey, lastKey = keys[0], keys[len(keys)-1]
	}
	if err := verifyPaths(proof.Left, proof.Right, firstKey, lastKey, root); err != nil {
		return err
	}

	// If proof.PathToKeys is empty, it means we have an empty range. This range
	// can be between keys, or outside of the range of existing keys.
	if len(keys) == 0 {
		// Range is outisde and to the left of existing keys.
		if proof.Left == nil && proof.Right.Path.isLeftmost() {
			return nil
		}
		// Range is outisde and to the right of existing keys.
		if proof.Right == nil && proof.Left.Path.isRightmost() {
			return nil
		}
		// Range is between two existing keys.
		if proof.Left != nil && proof.Right != nil {
			return nil
		}
		return errors.New("invalid proof of empty range")
	}

	// If we've reached this point, it means our range isn't empty, and we have
	// a list of keys.
	for i, path := range proof.PathToKeys {
		leafNode := IAVLProofLeafNode{KeyBytes: keys[i], ValueBytes: values[i]}
		if err := path.verify(leafNode, root); err != nil {
			return errors.Wrap(err, "failed to verify inner path")
		}
	}

	if !bytes.Equal(startKey, keys[0]) && proof.Left == nil {
		if !proof.PathToKeys[0].isLeftmost() {
			return errors.New("left path is nil and first inner path is not leftmost")
		}
	}

	// If the right proof is nil and the limit wasn't reached, we have to verify that
	// we're not missing any keys. Basically, if a key to the right is missing because
	// we've reached the limit, then it's fine. But if the key count is smaller than
	// the limit, we need a right proof to make sure no keys are missing.
	if proof.Right == nil && len(keys) != limit {
		if !bytes.Equal(endKey, lastKey) && !proof.PathToKeys[len(proof.PathToKeys)-1].isRightmost() {
			return errors.New("right path is nil and last inner path is not rightmost")
		}
	}
	return nil
}

func (proof *KeyRangeProof) String() string {
	paths := []string{}
	for _, p := range proof.PathToKeys {
		paths = append(paths, "\t"+p.String())
	}
	inner := fmt.Sprintf("PathToKeys: \n\t%s", strings.Join(paths, ",\n"))
	if proof.Left != nil {
		inner += fmt.Sprintf("Left: %#v (%x)\n", proof.Left.Node, proof.Left.Node.Hash())
	} else {
		inner += "Left: <nil>\n"
	}
	if proof.Right != nil {
		inner += fmt.Sprintf("RightNode: %#v (%x)", proof.Right.Node, proof.Right.Node.Hash())
	} else {
		inner += "Right: <nil>\n"
	}
	return "&KeyRangeProof{\n\t" + inner + "\n}"
}

// Returns a list of all paths, in order.
func (proof *KeyRangeProof) paths() []*PathToKey {
	paths := proof.PathToKeys[:]
	if proof.Left != nil {
		paths = append([]*PathToKey{proof.Left.Path}, paths...)
	}
	if proof.Right != nil {
		paths = append(paths, proof.Right.Path)
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

///////////////////////////////////////////////////////////////////////////////

func (t *IAVLTree) getRangeWithProof(keyStart, keyEnd []byte, limit int) (
	keys, values [][]byte, rangeProof *KeyRangeProof, err error,
) {
	if t.root == nil {
		return nil, nil, nil, errors.New("tree root is nil")
	}
	t.root.hashWithCount(t) // Ensure that all hashes are calculated.

	rangeProof = &KeyRangeProof{RootHash: t.root.hash}

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
		path, _, _ := t.root.pathToKey(t, k)
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
			path, _, _ := t.root.pathToKey(t, lkey)
			rangeProof.Left = &PathWithNode{
				Path: path,
				Node: IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval},
			}
		}
	}

	// Proof that the last key is the last value before keyEnd, or that we're limited.
	// If len(keys) == limit, it doesn't matter that a key exists to the right of the
	// last key, since we aren't interested in it.
	if !limited && (len(keys) == 0 || !bytes.Equal(keys[last], keyEnd)) {
		// Find index of first key to the right, and include proof if it isn't the
		// rightmost key.
		if idx, _, _ := t.Get(keyEnd); idx <= t.Size()-1 {
			rkey, rval := t.GetByIndex(idx)
			path, _, _ := t.root.pathToKey(t, rkey)
			rangeProof.Right = &PathWithNode{
				Path: path,
				Node: IAVLProofLeafNode{KeyBytes: rkey, ValueBytes: rval},
			}
		}
	}

	return keys, values, rangeProof, nil
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
			proof.Left = &PathWithNode{}
			proof.Left.Path, _, _ = t.root.pathToKey(t, k)
			proof.Left.Node = IAVLProofLeafNode{k, v}
		}
	}

	if !bytes.Equal(key, keyEnd) {
		if idx, _, exists := t.Get(keyEnd); idx <= t.Size()-1 && !exists {
			k, v := t.GetByIndex(idx)
			proof.Right = &PathWithNode{}
			proof.Right.Path, _, _ = t.root.pathToKey(t, k)
			proof.Right.Node = IAVLProofLeafNode{KeyBytes: k, ValueBytes: v}
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
			proof.Right = &PathWithNode{}
			proof.Right.Path, _, _ = t.root.pathToKey(t, k)
			proof.Right.Node = IAVLProofLeafNode{KeyBytes: k, ValueBytes: v}
		}
	}

	if !bytes.Equal(key, keyStart) {
		if idx, _, _ := t.Get(keyStart); idx-1 >= 0 && idx-1 <= t.Size()-1 {
			k, v := t.GetByIndex(idx - 1)
			proof.Left = &PathWithNode{}
			proof.Left.Path, _, _ = t.root.pathToKey(t, k)
			proof.Left.Node = IAVLProofLeafNode{k, v}
		}
	}

	return key, value, proof, nil
}

///////////////////////////////////////////////////////////////////////////////

// reverseIfDescending reverses the keys and values and swaps start and end key
// if startKey > endKey.
func reverseIfDescending(startKey, endKey []byte, keys, values [][]byte) (
	[]byte, []byte, [][]byte, [][]byte,
) {
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
	return startKey, endKey, keys, values
}
