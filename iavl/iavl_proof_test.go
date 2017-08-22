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
	_, proof, _ := tree.getWithProof([]byte("foo"))
	assert.Nil(t, proof)

	// insert lots of info and store the bytes
	keys := make([][]byte, 200)
	for i := 0; i < 200; i++ {
		key, value := randstr(20), randstr(200)
		tree.Set([]byte(key), []byte(value))
		keys[i] = []byte(key)
	}

	// query random key fails
	_, proof, _ = tree.getWithProof([]byte("foo"))
	assert.Nil(t, proof)

	// query min key fails
	_, proof, _ = tree.getWithProof([]byte{0})
	assert.Nil(t, proof)

	// valid proof for real keys
	root := tree.Hash()
	for _, key := range keys {
		value, proof, _ := tree.getWithProof(key)
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
		{0x11, 0xaa},
		{0xaa, 0x11},

		// Super-range, both directions.
		{0x0, 0xff},
		{0xff, 0x0},

		// Overlapping range, both directions.
		{0x12, 0xfa},
		{0xfa, 0x12},
		{0x04, 0xe8},
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
			var expected [][]byte
			tree.IterateRangeInclusive(startKey, endKey, ascending, func(k, v []byte) bool {
				expected = append(expected, k)
				return len(expected) == limit
			})

			keys, values, proof, err := tree.getRangeWithProof([]byte{c.startKey}, []byte{c.endKey}, limit)
			msg := fmt.Sprintf("range %x - %x with limit %d:\n%#v", c.startKey, c.endKey, limit, keys)
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
			root:          root,
			invalidProof:  &KeyRangeProof{RootHash: root},
			expectedError: errors.New("proof is incomplete"),
		},
		1: {
			keyStart:      []byte{0x0},
			keyEnd:        []byte{0xff},
			resultKeys:    [][]byte{{0x1}, {0x2}},
			resultVals:    [][]byte{{0x1}},
			root:          root,
			invalidProof:  &KeyRangeProof{RootHash: root},
			expectedError: errors.New("wrong number of keys or values for proof"),
		},
		2: { // An invalid proof with two adjacent paths which don't prove anything useful.
			keyStart: []byte{0x10},
			keyEnd:   []byte{0x30},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash:  root,
				LeftPath:  dummyPathToKey(tree, []byte{0x99}),
				LeftNode:  dummyLeafNode([]byte{0x99}, []byte{0x99}),
				RightPath: dummyPathToKey(tree, []byte{0xa1}),
				RightNode: dummyLeafNode([]byte{0xa1}, []byte{0xa1}),
			},
			expectedError: errors.New("left node must be lesser than start key"),
		},
		3: { // An invalid proof with one path.
			keyStart: []byte{0xf8},
			keyEnd:   []byte{0xf9},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				LeftPath: dummyPathToKey(tree, []byte{0xe4}),
				LeftNode: dummyLeafNode([]byte{0xe4}, []byte{0xe4}),
			},
			expectedError: errors.New("invalid proof of empty range"),
		},
		4: { // An invalid proof with one path.
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash:  root,
				RightPath: dummyPathToKey(tree, []byte{0xa}),
				RightNode: dummyLeafNode([]byte{0xa}, []byte{0xa}),
			},
			expectedError: errors.New("right node must be greater or equal than end key"),
		},
		5: { // An invalid proof with one path.
			keyStart: []byte{0x1},
			keyEnd:   []byte{0x2},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash:  root,
				RightPath: dummyPathToKey(tree, []byte{0x11}),
				RightNode: dummyLeafNode([]byte{0x11}, []byte{0x11}),
			},
			expectedError: errors.New("invalid proof of empty range"),
		},
		6: { // An invalid proof with one path.
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				LeftPath: dummyPathToKey(tree, []byte{0x99}),
				LeftNode: dummyLeafNode([]byte{0x99}, []byte{0x99}),
			},
			expectedError: errors.New("left node must be lesser than start key"),
		},
		7: {
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash:  root,
				LeftPath:  dummyPathToKey(tree, []byte{0x11}),
				LeftNode:  dummyLeafNode([]byte{0x11}, []byte{0x11}),
				RightPath: dummyPathToKey(tree, []byte{0xe4}),
				RightNode: dummyLeafNode([]byte{0xe4}, []byte{0xe4}),
			},
			expectedError: errors.New("paths #0 and #1 are not adjacent"),
		},
		8: {
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash:  root,
				LeftPath:  dummyPathToKey(tree, []byte{0x11}),
				LeftNode:  dummyLeafNode([]byte{0x01}, []byte{0x01}),
				RightPath: dummyPathToKey(tree, []byte{0x2e}),
				RightNode: dummyLeafNode([]byte{0x2e}, []byte{0x2e}),
			},
			expectedError: errors.New("failed to verify left path: leaf hashes do not match"),
		},
		9: {
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash:  root,
				LeftPath:  dummyPathToKey(tree, []byte{0x11}),
				LeftNode:  dummyLeafNode([]byte{0x11}, []byte{0x11}),
				RightPath: dummyPathToKey(tree, []byte{0x2e}),
				RightNode: dummyLeafNode([]byte{0x2f}, []byte{0x2f}),
			},
			expectedError: errors.New("failed to verify right path: leaf hashes do not match"),
		},
		10: {
			keyStart:   []byte{0x12},
			keyEnd:     []byte{0x50},
			resultKeys: [][]byte{[]byte{0x2e}},
			resultVals: [][]byte{[]byte{0x2e}},
			root:       root,
			invalidProof: &KeyRangeProof{
				RootHash:   root,
				PathToKeys: []*PathToKey{dummyPathToKey(tree, []byte{0x2e})},
				LeftPath:   dummyPathToKey(tree, []byte{0x11}),
				LeftNode:   dummyLeafNode([]byte{0x11}, []byte{0x11}),
			},
			expectedError: errors.New("right path is nil and last inner path is not rightmost"),
		},
		11: {
			keyStart:   []byte{0x12},
			keyEnd:     []byte{0x50},
			resultKeys: [][]byte{[]byte{0x2e}},
			resultVals: [][]byte{[]byte{0x2e}},
			root:       root,
			invalidProof: &KeyRangeProof{
				RootHash:   root,
				PathToKeys: []*PathToKey{dummyPathToKey(tree, []byte{0x2e})},
				LeftPath:   dummyPathToKey(tree, []byte{0x11}),
				LeftNode:   dummyLeafNode([]byte{0x11}, []byte{0x11}),
			},
			expectedError: errors.New("right path is nil and last inner path is not rightmost"),
		},
		12: {
			keyStart:   []byte{0x10},
			keyEnd:     []byte{0x30},
			resultKeys: [][]byte{[]byte{0x2e}},
			resultVals: [][]byte{[]byte{0x2e}},
			root:       root,
			invalidProof: &KeyRangeProof{
				RootHash:   root,
				PathToKeys: []*PathToKey{dummyPathToKey(tree, []byte{0x2e})},
				RightPath:  dummyPathToKey(tree, []byte{0x32}),
				RightNode:  dummyLeafNode([]byte{0x32}, []byte{0x32}),
			},
			expectedError: errors.New("left path is nil and first inner path is not leftmost"),
		},
		13: { // Construct an invalid proof with missing 0x2e and 0x32 keys.
			keyStart:   []byte{0x11},
			keyEnd:     []byte{0x50},
			resultKeys: [][]byte{[]byte{0x11}, []byte{0x50}},
			resultVals: [][]byte{[]byte{0x11}, []byte{0x50}},
			root:       root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				PathToKeys: []*PathToKey{
					dummyPathToKey(tree, []byte{0x11}),
					dummyPathToKey(tree, []byte{0x50}),
				},
			},
			expectedError: errors.New("paths #0 and #1 are not adjacent"),
		},
		14: {
			keyStart:   []byte{0x11},
			keyEnd:     []byte{0x50},
			resultKeys: [][]byte{[]byte{0x11}, []byte{0x2e}, []byte{0x32}},
			resultVals: [][]byte{[]byte{0x11}, []byte{0x2e}, []byte{0x32}},
			root:       root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				PathToKeys: []*PathToKey{
					dummyPathToKey(tree, []byte{0x11}),
					dummyPathToKey(tree, []byte{0x2e}),
					dummyPathToKey(tree, []byte{0x32}),
				},
				RightPath: dummyPathToKey(tree, []byte{0xf7}),
				RightNode: dummyLeafNode([]byte{0xf7}, []byte{0xf7}),
			},
			expectedError: errors.New("paths #2 and #3 are not adjacent"),
		},
		15: {
			keyStart:   []byte{0x11},
			keyEnd:     []byte{0x50},
			resultKeys: [][]byte{[]byte{0x2e}, []byte{0x32}, []byte{0x50}},
			resultVals: [][]byte{[]byte{0x2e}, []byte{0x32}, []byte{0x50}},
			root:       root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				PathToKeys: []*PathToKey{
					dummyPathToKey(tree, []byte{0x2e}),
					dummyPathToKey(tree, []byte{0x32}),
					dummyPathToKey(tree, []byte{0x50}),
				},
				LeftPath: dummyPathToKey(tree, []byte{0xa}),
				LeftNode: dummyLeafNode([]byte{0xa}, []byte{0xa}),
			},
			expectedError: errors.New("paths #0 and #1 are not adjacent"),
		},
		16: {
			keyStart:   []byte{0x11},
			keyEnd:     []byte{0x11},
			resultKeys: [][]byte{[]byte{0x11}},
			resultVals: [][]byte{[]byte{0x11}},
			root:       root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				PathToKeys: []*PathToKey{
					dummyPathToKey(tree, []byte{0x11}).dropRoot(),
				},
			},
			expectedError: errors.New("failed to verify inner path: path does not match supplied root"),
		},
		17: { // An invalid proof with one path and a limit.
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			limit:    10,
			invalidProof: &KeyRangeProof{
				RootHash:  root,
				RightPath: dummyPathToKey(tree, []byte{0x0a}),
				RightNode: dummyLeafNode([]byte{0x0a}, []byte{0x0a}),
			},
			expectedError: errors.New("right node must be greater or equal than end key"),
		},
		18: { // An invalid proof with one path and a limit.
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			limit:    10,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				LeftPath: dummyPathToKey(tree, []byte{0x99}),
				LeftNode: dummyLeafNode([]byte{0x99}, []byte{0x99}),
			},
			expectedError: errors.New("left node must be lesser than start key"),
		},
		19: { // First value returned is wrong. Should be 0x11.
			keyStart:   []byte{0x10},
			keyEnd:     []byte{0xf1},
			resultKeys: [][]byte{[]byte{0x2e}},
			resultVals: [][]byte{[]byte{0x2e}},
			root:       root,
			limit:      1,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				PathToKeys: []*PathToKey{
					dummyPathToKey(tree, []byte{0x2e}),
				},
			},
			expectedError: errors.New("left path is nil and first inner path is not leftmost"),
		},
	}

	for i, c := range cases {
		//
		// Test the case by checking we get the expected error.
		//
		err := c.invalidProof.Verify(c.keyStart, c.keyEnd, c.resultKeys, c.resultVals, c.root)
		require.Error(err, "test failed for case #%d", i)
		require.Equal(c.expectedError.Error(), err.Error(), "test failed for case #%d", i)

		//
		// Now do the same thing with start and end key swapped.
		//
		resultKeysDesc := [][]byte{}
		for _, k := range c.resultKeys {
			resultKeysDesc = append([][]byte{k}, resultKeysDesc...)
		}

		resultValsDesc := [][]byte{}
		for _, v := range c.resultVals {
			resultValsDesc = append([][]byte{v}, resultValsDesc...)
		}

		err = c.invalidProof.Verify(c.keyEnd, c.keyStart, resultKeysDesc, resultValsDesc, c.root)
		require.Error(err, "test failed for case #%d (reversed)", i)
		require.Equal(c.expectedError.Error(), err.Error(), "test failed for case #%d (reversed)", i)
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
	lval, lproof, _ := tree.getWithProof(lkey)
	require.NotNil(lproof)

	rkey := keys[2]
	rval, rproof, _ := tree.getWithProof(rkey)
	require.NotNil(rproof)

	missing := []byte{0x40}

	proof := &KeyAbsentProof{
		RootHash: lproof.RootHash,

		LeftPath: lproof.PathToKey,
		LeftNode: IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval},

		RightPath: rproof.PathToKey,
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
