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
	KeyExistsProof `json:"proof"`

	LeftPath *PathToKey        `json:"left_path"`
	LeftNode IAVLProofLeafNode `json:"left_node"`

	RightPath *PathToKey        `json:"right_path"`
	RightNode IAVLProofLeafNode `json:"right_node"`
}

// String returns a string representation of the proof.
func (proof *KeyFirstInRangeProof) String() string {
	return fmt.Sprintf("%#v", proof)
}

// Verify that the first in range proof is valid.
func (proof *KeyFirstInRangeProof) Verify(startKey, endKey, key, value []byte, root []byte) (err error) {
	if key != nil && (bytes.Compare(key, startKey) == -1 || bytes.Compare(key, endKey) == 1) {
		return InvalidInputsErr
	}
	if proof.LeftPath == nil && proof.RightPath == nil && proof.PathToKey == nil {
		return InvalidProofErr
	}

	if proof.LeftPath != nil {
		if err := proof.LeftPath.verify(proof.LeftNode, root); err != nil {
			return errors.Wrap(err, "failed to verify left path")
		}
		if !proof.LeftNode.isLesserThan(startKey) {
			return errors.New("left node must be lesser than start key")
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.Wrap(err, "failed to verify right path")
		}
		if !proof.RightNode.isGreaterThan(endKey) {
			return errors.New("right node must be greater than end key")
		}
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
		if proof.LeftPath != nil && proof.LeftPath.isLeftAdjacentTo(proof.PathToKey) {
			return
		}
	} else if proof.LeftPath != nil && proof.LeftPath.isRightmost() {
		// No key found. Range starts outside of the right boundary.
		return
	} else if proof.RightPath != nil && proof.RightPath.isLeftmost() {
		// No key found. Range ends outside of the left boundary.
		return
	} else if proof.LeftPath.isLeftAdjacentTo(proof.RightPath) {
		// No key found. Range is between two existing keys.
		return
	}

	return InvalidProofErr
}

///////////////////////////////////////////////////////////////////////////////

// KeyLastInRangeProof is a proof that a given key is the last in a given range.
type KeyLastInRangeProof struct {
	KeyExistsProof `json:"proof"`

	LeftPath *PathToKey        `json:"left_path"`
	LeftNode IAVLProofLeafNode `json:"left_node"`

	RightPath *PathToKey        `json:"right_path"`
	RightNode IAVLProofLeafNode `json:"right_node"`
}

// String returns a string representation of the proof.
func (proof *KeyLastInRangeProof) String() string {
	inner := ""
	if proof.PathToKey != nil {
		inner = fmt.Sprintf("PathToKey: \n\t%s", proof.PathToKey.String())
	}
	inner += fmt.Sprintf("LeftNode: %#v (%x)\n", proof.LeftNode, proof.LeftNode.Hash())
	inner += fmt.Sprintf("RightNode: %#v (%x)", proof.RightNode, proof.RightNode.Hash())
	return "&KeyLastRangeProof{\n\t" + inner + "\n}"
}

// Verify that the last in range proof is valid.
func (proof *KeyLastInRangeProof) Verify(startKey, endKey, key, value []byte, root []byte) (err error) {
	if key != nil && (bytes.Compare(key, startKey) == -1 || bytes.Compare(key, endKey) == 1) {
		return InvalidInputsErr
	}
	if proof.LeftPath == nil && proof.RightPath == nil && proof.PathToKey == nil {
		return InvalidProofErr
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
		if proof.RightPath != nil &&
			proof.PathToKey.isLeftAdjacentTo(proof.RightPath) {
			return
		}
	} else if proof.RightPath != nil && proof.RightPath.isLeftmost() {
		return
	} else if proof.LeftPath != nil && proof.LeftPath.isRightmost() {
		return
	} else if proof.LeftPath.isLeftAdjacentTo(proof.RightPath) {
		return
	}
	return InvalidProofErr
}

///////////////////////////////////////////////////////////////////////////////

// KeyRangeProof is proof that a range of keys does or does not exist.
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
		if !proof.LeftNode.isLesserThan(firstKey) {
			return errors.New("left node must be lesser than start key")
		}
	}
	if proof.RightPath != nil {
		if err := proof.RightPath.verify(proof.RightNode, root); err != nil {
			return errors.Wrap(err, "failed to verify right path")
		}
		if !bytes.Equal(proof.RightNode.KeyBytes, lastKey) && !proof.RightNode.isGreaterThan(lastKey) {
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
