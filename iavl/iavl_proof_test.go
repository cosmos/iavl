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
	err = absenceProof.Verify(key, nil, root)
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

func TestIAVLTreeKeyInRangeProofs(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)
	require := require.New(t)
	for _, ikey := range []byte{
		0x0a, 0x11, 0x2e, 0x32, 0x50, 0x72, 0x99, 0xa1, 0xe4, 0xf7,
	} {
		key := []byte{ikey}
		tree.Set(key, key)
	}
	root := tree.Hash()

	cases := []struct {
		startKey byte
		endKey   byte
		first    []byte
		last     []byte
	}{
		{startKey: 0x0a, endKey: 0xf7, first: []byte{0x0a}, last: []byte{0xf7}},
		{startKey: 0x0, endKey: 0xff, first: []byte{0x0a}, last: []byte{0xf7}},
		{startKey: 0x14, endKey: 0xf1, first: []byte{0x2e}, last: []byte{0xe4}},
		{startKey: 0x2e, endKey: 0x32, first: []byte{0x2e}, last: []byte{0x32}},
		{startKey: 0x2f, endKey: 0x32, first: []byte{0x32}, last: []byte{0x32}},
		{startKey: 0x2e, endKey: 0x31, first: []byte{0x2e}, last: []byte{0x2e}},
		{startKey: 0x12, endKey: 0x31, first: []byte{0x2e}, last: []byte{0x2e}},
		{startKey: 0xf8, endKey: 0xff, first: nil, last: nil},
		{startKey: 0x12, endKey: 0x20, first: nil, last: nil},
		{startKey: 0x0, endKey: 0x09, first: nil, last: nil},
	}

	for _, c := range cases {
		startKey := []byte{c.startKey}
		endKey := []byte{c.endKey}

		// Test first-in-range.
		key, val, firProof, err := tree.GetFirstInRangeWithProof(startKey, endKey)
		msg := fmt.Sprintf("first in range %x - %x: %x", c.startKey, c.endKey, key)
		require.NoError(err, "%+v", err)
		require.Equal(c.first, key, "Key returned not equal for %s", msg)
		require.Equal(key, val)
		err = firProof.Verify(startKey, endKey, key, val, root)
		require.NoError(err, "Got error '%v' for %s\n%s", err, msg, firProof.String())

		// Test last-in-range.
		key, val, lirProof, err := tree.GetLastInRangeWithProof(startKey, endKey)
		msg = fmt.Sprintf("last in range %x - %x: %x", c.startKey, c.endKey, key)
		require.NoError(err, "%+v", err)
		require.Equal(c.last, key, "Key returned not equal for %s", msg)
		require.Equal(key, val)
		err = lirProof.Verify(startKey, endKey, key, val, root)
		require.NoError(err, "Got error '%v' for %s", err, msg)
	}
}

func TestIAVLTreeKeyFirstInRangeProofsVerify(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)
	require := require.New(t)
	for _, ikey := range []byte{
		0x0a, 0x11, 0x2e, 0x32, 0x50, 0x72, 0x99, 0xa1, 0xe4, 0xf7,
	} {
		key := []byte{ikey}
		tree.Set(key, key)
	}
	root := tree.Hash()

	cases := [...]struct {
		startKey, endKey     []byte
		resultKey, resultVal []byte
		root                 []byte
		invalidProof         *KeyFirstInRangeProof
		expectedError        error
	}{
		0: { // Left path is invalid.
			root:      root,
			startKey:  []byte{0x30},
			endKey:    []byte{0xff},
			resultKey: []byte{0x72},
			resultVal: []byte{0x72},
			invalidProof: &KeyFirstInRangeProof{
				KeyExistsProof: KeyExistsProof{
					RootHash:  root,
					PathToKey: dummyPathToKey(tree, []byte{0x72}),
				},
				Left: &PathWithNode{
					dummyPathToKey(tree, []byte{0x50}),
					dummyLeafNode([]byte{0x11}, []byte{0x11}),
				},
			},
			expectedError: errors.New("failed to verify left path: invalid path"),
		},
	}

	for i, c := range cases {
		err := c.invalidProof.Verify(c.startKey, c.endKey, c.resultKey, c.resultVal, c.root)
		require.Error(err, "test failed for case #%d", i)
		require.Equal(c.expectedError.Error(), err.Error(), "test failed for case #%d", i)
	}
}

func TestIAVLTreeKeyLastInRangeProofsVerify(t *testing.T) {
	var tree *IAVLTree = NewIAVLTree(0, nil)
	require := require.New(t)
	for _, ikey := range []byte{
		0x0a, 0x11, 0x2e, 0x32, 0x50, 0x72, 0x99, 0xa1, 0xe4, 0xf7,
	} {
		key := []byte{ikey}
		tree.Set(key, key)
	}
	root := tree.Hash()

	cases := [...]struct {
		startKey, endKey     []byte
		resultKey, resultVal []byte
		root                 []byte
		invalidProof         *KeyLastInRangeProof
		expectedError        error
	}{
		0: {
			root:      root,
			startKey:  []byte{0x0},
			endKey:    []byte{0xff},
			resultKey: []byte{0x11},
			resultVal: []byte{0x11},
			invalidProof: &KeyLastInRangeProof{
				KeyExistsProof: KeyExistsProof{
					RootHash:  root,
					PathToKey: dummyPathToKey(tree, []byte{0x11}),
				},
			},
			expectedError: ErrInvalidProof,
		},
		1: { // Result is outside of the range (right).
			root:      root,
			startKey:  []byte{0x10},
			endKey:    []byte{0xf6},
			resultKey: []byte{0xf7},
			resultVal: []byte{0xf7},
			invalidProof: &KeyLastInRangeProof{
				KeyExistsProof: KeyExistsProof{
					RootHash:  root,
					PathToKey: dummyPathToKey(tree, []byte{0xf7}),
				},
			},
			expectedError: ErrInvalidInputs,
		},
		2: { // Result is outside of the range (left).
			root:      root,
			startKey:  []byte{0x10},
			endKey:    []byte{0xf6},
			resultKey: []byte{0x0a},
			resultVal: []byte{0x0a},
			invalidProof: &KeyLastInRangeProof{
				KeyExistsProof: KeyExistsProof{
					RootHash:  root,
					PathToKey: dummyPathToKey(tree, []byte{0x0a}),
				},
			},
			expectedError: ErrInvalidInputs,
		},
		3: { // Right node is greater than end key.
			root:      root,
			startKey:  []byte{0x10},
			endKey:    []byte{0xf6},
			resultKey: []byte{0x11},
			resultVal: []byte{0x11},
			invalidProof: &KeyLastInRangeProof{
				KeyExistsProof: KeyExistsProof{
					RootHash:  root,
					PathToKey: dummyPathToKey(tree, []byte{0x11}),
				},
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xf7}),
					Node: dummyLeafNode([]byte{0xf7}, []byte{0xf7}),
				},
			},
			expectedError: ErrInvalidProof,
		},
		4: {
			root:      root,
			startKey:  []byte{0x10},
			endKey:    []byte{0xf6},
			resultKey: nil,
			resultVal: nil,
			invalidProof: &KeyLastInRangeProof{
				KeyExistsProof: KeyExistsProof{
					RootHash: root,
				},
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xa}),
					Node: dummyLeafNode([]byte{0xa}, []byte{0xa}),
				},
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xf7}),
					Node: dummyLeafNode([]byte{0xf7}, []byte{0xf7}),
				},
			},
			expectedError: ErrInvalidProof,
		},
		5: {
			root:      root,
			startKey:  []byte{0x10},
			endKey:    []byte{0xf6},
			resultKey: []byte{0xa1},
			resultVal: []byte{0xa1},
			invalidProof: &KeyLastInRangeProof{
				KeyExistsProof: KeyExistsProof{
					RootHash:  root,
					PathToKey: dummyPathToKey(tree, []byte{0xa1}),
				},
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xa}),
					Node: dummyLeafNode([]byte{0xa}, []byte{0xa}),
				},
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xe4}),
					Node: dummyLeafNode([]byte{0xe4}, []byte{0xe4}),
				},
			},
			expectedError: errors.New("right node must be greater than end key"),
		},
	}

	for i, c := range cases {
		err := c.invalidProof.Verify(c.startKey, c.endKey, c.resultKey, c.resultVal, c.root)
		require.Error(err, "test failed for case #%d", i)
		require.Equal(c.expectedError.Error(), err.Error(), "test failed for case #%d", i)
	}
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
			err = proof.Verify([]byte{c.startKey}, []byte{c.endKey}, limit, keys, values, root)
			require.NoError(err, "Got error '%v' for %s:\n%s", err, msg, proof.String())
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
			expectedError: ErrInvalidProof,
		},
		1: {
			keyStart:      []byte{0x0},
			keyEnd:        []byte{0xff},
			resultKeys:    [][]byte{{0x1}, {0x2}},
			resultVals:    [][]byte{{0x1}},
			root:          root,
			invalidProof:  &KeyRangeProof{RootHash: root},
			expectedError: ErrInvalidInputs,
		},
		2: { // An invalid proof with two adjacent paths which don't prove anything useful.
			keyStart: []byte{0x10},
			keyEnd:   []byte{0x30},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x99}),
					Node: dummyLeafNode([]byte{0x99}, []byte{0x99}),
				},
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xa1}),
					Node: dummyLeafNode([]byte{0xa1}, []byte{0xa1}),
				},
			},
			expectedError: errors.New("left node must be lesser than start key"),
		},
		3: { // An invalid proof with one path.
			keyStart: []byte{0xf8},
			keyEnd:   []byte{0xf9},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xe4}),
					Node: dummyLeafNode([]byte{0xe4}, []byte{0xe4}),
				},
			},
			expectedError: ErrInvalidProof,
		},
		4: { // An invalid proof with one path.
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xa}),
					Node: dummyLeafNode([]byte{0xa}, []byte{0xa}),
				},
			},
			expectedError: errors.New("right node must be greater than end key"),
		},
		5: { // An invalid proof with one path.
			keyStart: []byte{0x1},
			keyEnd:   []byte{0x2},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x11}),
					Node: dummyLeafNode([]byte{0x11}, []byte{0x11}),
				},
			},
			expectedError: ErrInvalidProof,
		},
		6: { // An invalid proof with one path.
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x99}),
					Node: dummyLeafNode([]byte{0x99}, []byte{0x99}),
				},
			},
			expectedError: errors.New("left node must be lesser than start key"),
		},
		7: {
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x11}),
					Node: dummyLeafNode([]byte{0x11}, []byte{0x11}),
				},
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xe4}),
					Node: dummyLeafNode([]byte{0xe4}, []byte{0xe4}),
				},
			},
			expectedError: errors.New("paths #0 and #1 are not adjacent"),
		},
		8: {
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x11}),
					Node: dummyLeafNode([]byte{0x01}, []byte{0x01}),
				},
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x2e}),
					Node: dummyLeafNode([]byte{0x2e}, []byte{0x2e}),
				},
			},
			expectedError: errors.New("failed to verify left path: invalid path"),
		},
		9: {
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x11}),
					Node: dummyLeafNode([]byte{0x11}, []byte{0x11}),
				},
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x2e}),
					Node: dummyLeafNode([]byte{0x2f}, []byte{0x2f}),
				},
			},
			expectedError: errors.New("failed to verify right path: invalid path"),
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
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x11}),
					Node: dummyLeafNode([]byte{0x11}, []byte{0x11}),
				},
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
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x11}),
					Node: dummyLeafNode([]byte{0x11}, []byte{0x11}),
				},
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
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x32}),
					Node: dummyLeafNode([]byte{0x32}, []byte{0x32}),
				},
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
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xf7}),
					Node: dummyLeafNode([]byte{0xf7}, []byte{0xf7}),
				},
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
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0xa}),
					Node: dummyLeafNode([]byte{0xa}, []byte{0xa}),
				},
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
			expectedError: errors.New("failed to verify inner path: invalid path"),
		},
		17: { // An invalid proof with one path and a limit.
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			limit:    10,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Right: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x0a}),
					Node: dummyLeafNode([]byte{0x0a}, []byte{0x0a}),
				},
			},
			expectedError: errors.New("right node must be greater than end key"),
		},
		18: { // An invalid proof with one path and a limit.
			keyStart: []byte{0x30},
			keyEnd:   []byte{0x40},
			root:     root,
			limit:    10,
			invalidProof: &KeyRangeProof{
				RootHash: root,
				Left: &PathWithNode{
					Path: dummyPathToKey(tree, []byte{0x99}),
					Node: dummyLeafNode([]byte{0x99}, []byte{0x99}),
				},
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
		err := c.invalidProof.Verify(c.keyStart, c.keyEnd, c.limit, c.resultKeys, c.resultVals, c.root)
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

		err = c.invalidProof.Verify(c.keyEnd, c.keyStart, c.limit, resultKeysDesc, resultValsDesc, c.root)
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

			err = proof.Verify(key, nil, root)
			require.NoError(err, "Got verification error for 0x%x: %+v", key, err)

			if bytes.Compare(key, min) < 0 {
				require.Nil(proof.Left)
				require.NotNil(proof.Right)
			} else if bytes.Compare(key, max) > 0 {
				require.Nil(proof.Right)
				require.NotNil(proof.Left)
			} else {
				require.NotNil(proof.Left)
				require.NotNil(proof.Right)
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

		Left: &PathWithNode{
			Path: lproof.PathToKey,
			Node: IAVLProofLeafNode{KeyBytes: lkey, ValueBytes: lval},
		},
		Right: &PathWithNode{
			Path: rproof.PathToKey,
			Node: IAVLProofLeafNode{KeyBytes: rkey, ValueBytes: rval},
		},
	}
	err := proof.Verify(missing, nil, tree.Hash())
	require.Error(err, "Proof should not verify")

	proof, err = tree.keyAbsentProof(missing)
	require.NoError(err)
	require.NotNil(proof)

	err = proof.Verify(missing, nil, tree.Hash())
	require.NoError(err)

	err = proof.Verify([]byte{0x45}, nil, tree.Hash())
	require.NoError(err)

	err = proof.Verify([]byte{0x25}, nil, tree.Hash())
	require.Error(err)
}
