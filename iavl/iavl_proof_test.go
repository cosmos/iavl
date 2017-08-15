package iavl

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"testing"
)

func TestIAVLTreeGetWithProof(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)
	require := require.New(t)
	keys := [][]byte{}
	for _, ikey := range []byte{0x11, 0x32, 0x50, 0x72, 0x99} {
		key := []byte{ikey}
		keys = append(keys, key)
		tree.Set(key, []byte(randstr(8)))
	}
	root := tree.Hash()

	key := []byte{0x32}
	val, existProof, absenceProof, err := tree.GetWithProof(key)
	require.NotEmpty(val)
	require.NotNil(existProof)
	err = existProof.Verify(key, val, root)
	require.NoError(err, "%+v", err)
	require.Nil(absenceProof)
	require.NoError(err)

	key = []byte{0x1}
	val, existProof, absenceProof, err = tree.GetWithProof(key)
	require.Empty(val)
	require.Nil(existProof)
	require.NotNil(absenceProof)
	err = absenceProof.Verify(key, root)
	require.NoError(err, "%+v", err)
	require.NoError(err)
}

func reverseBytes(xs [][]byte) [][]byte {
	reversed := [][]byte{}
	for i := len(xs) - 1; i >= 0; i-- {
		reversed = append(reversed, xs[i])
	}
	return reversed
}

func TestIAVLTreeKeyExistsProof(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)

	// should get false for proof with nil root
	_, proof, _ := tree.getWithKeyExistsProof([]byte("foo"))
	assert.Nil(t, proof)

	// insert lots of info and store the bytes
	keys := make([][]byte, 200)
	for i := 0; i < 200; i++ {
		key, value := randstr(20), randstr(200)
		tree.Set([]byte(key), []byte(value))
		keys[i] = []byte(key)
	}

	// query random key fails
	_, proof, _ = tree.getWithKeyExistsProof([]byte("foo"))
	assert.Nil(t, proof)

	// query min key fails
	_, proof, _ = tree.getWithKeyExistsProof([]byte{0})
	assert.Nil(t, proof)

	// valid proof for real keys
	root := tree.Hash()
	for _, key := range keys {
		value, proof, _ := tree.getWithKeyExistsProof(key)
		assert.NotEmpty(t, value)
		if assert.NotNil(t, proof) {
			err := proof.Verify(key, value, root)
			assert.NoError(t, err, "%+v", err)
		}
	}
	// TODO: Test with single value in tree.
}

func TestIAVLTreeKeyRangeProof(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)
	require := require.New(t)
	keys := [][]byte{}
	for _, ikey := range []byte{
		0x0a, 0x11, 0x2e, 0x32, 0x50, 0x72, 0x99, 0xa1, 0xe4, 0xf7,
	} {
		key := []byte{ikey}
		keys = append(keys, key)
		tree.Set(key, []byte(randstr(8)))
	}
	root := tree.Hash()

	cases := []struct {
		startKey byte
		endKey   byte
	}{
		// Full range, existing keys, both directions.
		{0x0a, 0xf7},
		{0xf7, 0x0a},

		// Sub-range, existing keys, both directions.
		{0x2e, 0xa1},
		{0xa1, 0x2e},

		// Sub-range, non-existing keys, both directions.
		{0x2f, 0xa0},
		{0xa0, 0x2f},

		// Sub-range, partially-existing keys, both directions.
		{0x2f, 0xa1},
		{0xa1, 0x2f},

		// Super-range, both directions.
		{0x0, 0xff},
		{0xff, 0x0},

		// Overlapping range, both directions.
		{0x12, 0xfa},
		{0xe8, 0x04},

		// Equal keys.
		{0x72, 0x72},

		// Empty range.
		{0x60, 0x70},
		{0x70, 0x60},

		// Empty range outside of left boundary.
		{0x01, 0x03},
		{0x03, 0x01},

		// Empty range outside of right boundary.
		{0xf9, 0xfd},
		{0xfd, 0xf9},
	}

	for _, c := range cases {
		startKey := []byte{c.startKey}
		endKey := []byte{c.endKey}
		ascending := bytes.Compare(startKey, endKey) == -1
		if !ascending {
			startKey, endKey = endKey, startKey
		}

		for limit := -1; limit < len(keys); limit++ {
			expected := [][]byte{}
			tree.IterateRangeInclusive(startKey, endKey, ascending, func(k, v []byte) bool {
				expected = append(expected, k)
				return len(expected) == limit
			})

			keys, values, proof, err := tree.getWithKeyRangeProof([]byte{c.startKey}, []byte{c.endKey}, limit)
			msg := fmt.Sprintf("range %x - %x with limit %d:\n%#v\n\n%s", c.startKey, c.endKey, limit, keys, proof.String())
			require.NoError(err, "%+v", err)
			require.Equal(expected, keys, "Keys returned not equal for %s", msg)
			err = proof.Verify([]byte{c.startKey}, []byte{c.endKey}, keys, values, root)
			require.NoError(err, "Got error '%v' for %s", err, msg)
		}
	}
}

func TestIAVLTreeKeyRangeProofVerify(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)
	require := require.New(t)
	keys := [][]byte{}
	values := [][]byte{}
	for _, ikey := range []byte{
		0x0a, 0x11, 0x2e, 0x32, 0x50, 0x72, 0x99, 0xa1, 0xe4, 0xf7,
	} {
		key, val := []byte{ikey}, []byte{ikey}
		keys, values = append(keys, key), append(values, val)
		tree.Set(key, val)
	}
	root := tree.Hash()

	// Construct a proof with a missing value 0x32 in the range.
	expected := errors.New("paths 1 and 2 are not adjacent")
	startKey, endKey := []byte{0x11}, []byte{0x50}
	keys, vals, proof, err := tree.getWithKeyRangeProof(startKey, endKey, -1)
	require.NoError(err)
	missingIdx := 2
	invalidKeys := append(keys[:missingIdx], keys[missingIdx+1:]...)
	invalidVals := append(vals[:missingIdx], vals[missingIdx+1:]...)
	invalidPaths := append(proof.PathToKeys[:missingIdx], proof.PathToKeys[missingIdx+1:]...)
	invalidProof := &KeyRangeProof{
		RootHash:   root,
		PathToKeys: invalidPaths,

		LeftPath: proof.LeftPath,
		LeftNode: proof.LeftNode,

		RightPath: proof.RightPath,
		RightNode: proof.RightNode,
	}
	err = invalidProof.Verify(startKey, endKey, invalidKeys, invalidVals, root)
	require.Error(err)
	require.EqualValues(expected.Error(), err.Error(), "Expected verification error")

	// Construct a proof and try to verify with a range greater than the proof.
	expected = errors.New("left path is nil and first inner path is not leftmost")
	startKey, endKey = []byte{0x2e}, []byte{0x50}
	keys, vals, proof, err = tree.getWithKeyRangeProof(startKey, endKey, -1)
	proof.PathToKeys = proof.PathToKeys[1:]
	err = proof.Verify([]byte{0x11}, []byte{0x50}, keys[1:], vals[1:], root)
	require.Error(err)
	require.EqualValues(expected.Error(), err.Error(), "Expected verification error")

	expected = errors.New("first inner path isn't adjacent to left path")
	startKey, endKey = []byte{0x12}, []byte{0x50}
	keys, vals, proof, err = tree.getWithKeyRangeProof(startKey, endKey, -1)
	val, wrongProof, err := tree.getWithKeyExistsProof([]byte{0x0a})
	require.NoError(err)
	proof.LeftPath = &wrongProof.PathToKey
	proof.LeftNode = IAVLProofLeafNode{[]byte{0x0a}, val}
	err = proof.Verify(startKey, endKey, keys, vals, root)
	require.Error(err)
	require.EqualValues(expected.Error(), err.Error(), "Expected verification error")

	expected = errors.New("left node key must be lesser than start key")
	startKey, endKey = []byte{0x12}, []byte{0x50}
	keys, vals, proof, err = tree.getWithKeyRangeProof(startKey, endKey, -1)
	val, wrongProof, err = tree.getWithKeyExistsProof([]byte{0x2e})
	require.NoError(err)
	proof.LeftPath = &wrongProof.PathToKey
	proof.LeftNode = IAVLProofLeafNode{[]byte{0x2e}, val}
	err = proof.Verify(startKey, endKey, keys, vals, root)
	require.Error(err)
	require.EqualValues(expected.Error(), err.Error(), "Expected verification error")

	cases := [...]struct {
		keyStart, keyEnd       []byte
		limit                  int
		resultKeys, resultVals [][]byte
		root                   []byte
		invalidProof           *KeyRangeProof
		expectedError          error
	}{
		0: {
			keyStart:      []byte{0x0},
			keyEnd:        []byte{0xff},
			limit:         -1,
			resultKeys:    nil,
			resultVals:    nil,
			root:          root,
			invalidProof:  &KeyRangeProof{RootHash: root},
			expectedError: errors.New("proof is incomplete"),
		},
		1: {
			keyStart:      []byte{0x0},
			keyEnd:        []byte{0xff},
			limit:         -1,
			resultKeys:    [][]byte{{0x1}, {0x2}},
			resultVals:    [][]byte{{0x1}},
			root:          root,
			invalidProof:  &KeyRangeProof{RootHash: root},
			expectedError: errors.New("wrong number of keys or values for proof"),
		},
		2: { // An invalid proof with two adjacent paths which don't prove anything useful.
			keyStart:   []byte{0x10},
			keyEnd:     []byte{0x30},
			limit:      -1,
			resultKeys: nil,
			resultVals: nil,
			root:       root,
			invalidProof: &KeyRangeProof{
				RootHash:  root,
				LeftPath:  dummyPathToKey(tree, []byte{0x99}),
				LeftNode:  dummyLeafNode([]byte{0x99}, []byte{0x99}),
				RightPath: dummyPathToKey(tree, []byte{0xa1}),
				RightNode: dummyLeafNode([]byte{0xa1}, []byte{0xa1}),
			},
			expectedError: errors.New("start and end key are not between left and right node"),
		},
	}
	for i, c := range cases {
		err := c.invalidProof.Verify(c.keyStart, c.keyEnd, c.resultKeys, c.resultVals, c.root)
		require.Error(err, "test failed for case #%d", i)
		require.Equal(c.expectedError.Error(), err.Error(), "test failed for case #%d", i)
	}
}

func TestIAVLTreeKeyAbsentProof(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)
	require := require.New(t)

	proof, err := tree.keyAbsentProof([]byte{0x1})
	require.Nil(proof, "Proof should be nil for empty tree")
	require.Error(err)

	keys := [][]byte{}
	for _, ikey := range []byte{0x11, 0x32, 0x50, 0x72, 0x99} {
		key := []byte{ikey}
		keys = append(keys, key)
		tree.Set(key, []byte(randstr(8)))
	}
	root := tree.Hash()

	// Get min and max keys.
	min, _ := tree.GetByIndex(0)
	max, _ := tree.GetByIndex(tree.Size() - 1)

	// Go through a range of keys and test the result of creating non-existence
	// proofs for them.

	for i := min[0] - 1; i < max[0]+1; i++ {
		key := []byte{i}
		exists := false

		for _, k := range keys {
			if bytes.Compare(key, k) == 0 {
				exists = true
				break
			}
		}

		if exists {
			proof, err = tree.keyAbsentProof(key)
			require.Nil(proof, "Proof should be nil for existing key")
			require.Error(err, "Got verification error for 0x%x: %+v", key, err)
		} else {
			proof, err = tree.keyAbsentProof(key)
			require.NotNil(proof, "Proof should not be nil for non-existing key")
			require.NoError(err, "%+v", err)

			err = proof.Verify(key, root)
			require.NoError(err, "Got verification error for 0x%x: %+v", key, err)

			if bytes.Compare(key, min) < 0 {
				require.Nil(proof.LeftPath)
				require.NotNil(proof.RightPath)
			} else if bytes.Compare(key, max) > 0 {
				require.Nil(proof.RightPath)
				require.NotNil(proof.LeftPath)
			} else {
				require.NotNil(proof.LeftPath)
				require.NotNil(proof.RightPath)
			}
		}
	}
}

func TestKeyAbsentProofVerify(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)
	require := require.New(t)
	keys := [][]byte{}
	for _, ikey := range []byte{0x11, 0x32, 0x50, 0x72, 0x99} {
		key := []byte{ikey}
		keys = append(keys, key)
		tree.Set(key, []byte(randstr(8)))
	}

	// Create a bogus non-existence proof and check that it does not verify.

	lkey := keys[0]
	lval, lproof, _ := tree.getWithKeyExistsProof(lkey)
	require.NotNil(lproof)

	rkey := keys[2]
	rval, rproof, _ := tree.getWithKeyExistsProof(rkey)
	require.NotNil(rproof)

	missing := []byte{0x40}

	proof := &KeyAbsentProof{
		RootHash: lproof.RootHash,

		LeftPath: &lproof.PathToKey,
		LeftNode: IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval},

		RightPath: &rproof.PathToKey,
		RightNode: IAVLProofLeafNode{KeyBytes: rkey, ValueBytes: rval},
	}
	err := proof.Verify(missing, tree.Hash())
	require.Error(err, "Proof should not verify")

	proof, err = tree.keyAbsentProof(missing)
	require.NoError(err)
	require.NotNil(proof)

	err = proof.Verify(missing, tree.Hash())
	require.NoError(err)

	err = proof.Verify([]byte{0x45}, tree.Hash())
	require.NoError(err)

	err = proof.Verify([]byte{0x25}, tree.Hash())
	require.Error(err)
}
