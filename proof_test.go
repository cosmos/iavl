package iavl

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/libs/test"
)

func TestTreeGetWithProof(t *testing.T) {
	tree := NewTree(nil, 0)
	require := require.New(t)
	for _, ikey := range []byte{0x11, 0x32, 0x50, 0x72, 0x99} {
		key := []byte{ikey}
		tree.Set(key, []byte(rand.Str(8)))
	}
	root := tree.Hash()

	key := []byte{0x32}
	val, proof, err := tree.GetWithProof(key)
	require.NoError(err)
	require.NotEmpty(val)
	require.NotNil(proof)
	err = proof.Verify(root)
	require.NoError(err, "%+v", err)
	err = proof.VerifyItem(0, key, val)
	require.NoError(err, "%+v", err)

	key = []byte{0x1}
	val, proof, err = tree.GetWithProof(key)
	require.NoError(err)
	require.Empty(val)
	require.NotNil(proof)
	err = proof.Verify(root)
	require.NoError(err, "%+v", err)
	err = proof.VerifyAbsence(key)
	require.NoError(err, "%+v", err)
}

func TestTreeKeyExistsProof(t *testing.T) {
	tree := NewTree(nil, 0)
	root := tree.Hash()

	// should get false for proof with nil root
	proof, _, err := tree.getRangeProof([]byte("foo"), nil, 1)
	assert.NotNil(t, err)
	assert.NotNil(t, proof.Verify(root))

	// insert lots of info and store the bytes
	keys := make([][]byte, 200)
	for i := 0; i < 200; i++ {
		key := randstr(20)
		value := "value_for_" + key
		tree.Set([]byte(key), []byte(value))
		keys[i] = []byte(key)
	}
	sortByteSlices(keys) // Sort keys
	root = tree.Hash()

	// query random key fails
	proof, _, err = tree.getRangeProof([]byte("foo"), nil, 2)
	assert.Nil(t, err)
	assert.Nil(t, proof.Verify(root))
	assert.Nil(t, proof.VerifyAbsence([]byte("foo")), proof.String())

	// query min key fails
	proof, _, err = tree.getRangeProof([]byte{0x00}, []byte{0x00}, 2)
	assert.Nil(t, err)
	assert.Nil(t, proof.Verify(root))
	assert.Nil(t, proof.VerifyAbsence([]byte{0x00}))

	// valid proof for real keys
	for i, key := range keys {
		var values [][]byte
		proof, values, err = tree.getRangeProof(key, nil, 2)
		require.Nil(t, err)

		require.Equal(t,
			append([]byte("value_for_"), key...),
			values[0],
		)
		require.Nil(t, proof.Verify(root))
		require.Nil(t, proof.VerifyAbsence(cpIncr(key)))
		if i < len(keys)-1 {
			// There should be 2 items as per limit.
			if len(values) != 2 {
				printNode(tree.ndb, tree.root, 0)
			}
			require.Equal(t, 2, len(values), proof.String())
			if i < len(keys)-2 {
				// No last item... not a proof of absence of large key.
				require.NotNil(t, proof.VerifyAbsence(bytes.Repeat([]byte{0xFF}, 20)), proof.String())
			} else {
				// Last item is included.
				require.Nil(t, proof.VerifyAbsence(bytes.Repeat([]byte{0xFF}, 20)))
			}
		} else {
			// There should be 1 item since no more can be queried.
			require.Equal(t, 1, len(values), values)
			// last item of tree... valid proof of absence of large key.
			require.Nil(t, proof.VerifyAbsence(bytes.Repeat([]byte{0xFF}, 20)))
		}
	}
	// TODO: Test with single value in tree.
}

func TestTreeKeyInRangeProofs(t *testing.T) {
	tree := NewTree(nil, 0)
	require := require.New(t)
	keys := []byte{0x0a, 0x11, 0x2e, 0x32, 0x50, 0x72, 0x99, 0xa1, 0xe4, 0xf7} // 10 total.
	for _, ikey := range keys {
		key := []byte{ikey}
		tree.Set(key, key)
	}
	root := tree.Hash()

	cases := []struct {
		startKey byte
		endKey   byte
		keys     []byte // one byte per key.
	}{
		{startKey: 0x0a, endKey: 0xf7, keys: keys[:10]},
		{startKey: 0x0a, endKey: 0xf8, keys: keys[:10]},
		{startKey: 0x0, endKey: 0xff, keys: keys[:]},
		{startKey: 0x14, endKey: 0xe4, keys: keys[1:9]},
		{startKey: 0x14, endKey: 0xe5, keys: keys[1:9]},
		{startKey: 0x14, endKey: 0xe6, keys: keys[1:10]},
		{startKey: 0x14, endKey: 0xf1, keys: keys[1:10]},
		{startKey: 0x14, endKey: 0xf7, keys: keys[1:10]},
		{startKey: 0x14, endKey: 0xff, keys: keys[1:10]},
		{startKey: 0x2e, endKey: 0x31, keys: keys[2:4]},
		{startKey: 0x2e, endKey: 0x32, keys: keys[2:4]},
		{startKey: 0x2f, endKey: 0x32, keys: keys[2:4]},
		{startKey: 0x2e, endKey: 0x31, keys: keys[2:4]},
		{startKey: 0x2e, endKey: 0x2f, keys: keys[2:3]},
		{startKey: 0x12, endKey: 0x31, keys: keys[1:4]},
		{startKey: 0xf8, endKey: 0xff, keys: keys[9:10]},
		{startKey: 0x12, endKey: 0x20, keys: keys[1:3]},
		{startKey: 0x0, endKey: 0x09, keys: keys[0:1]},
	}

	// fmt.Println("PRINT TREE")
	// printNode(tree.ndb, tree.root, 0)
	// fmt.Println("PRINT TREE END")

	for i, c := range cases {
		t.Logf("case %v", i)
		startKey := []byte{c.startKey}
		endKey := []byte{c.endKey}

		// Compute range proof.
		keys, values, proof, err := tree.GetRangeWithProof(startKey, endKey, 0)
		require.NoError(err, "%+v", err)
		require.Equal(c.keys, flatten(keys))
		require.Equal(c.keys, flatten(values))

		// Verify that proof is valid.
		err = proof.Verify(root)

		require.NoError(err, "%+v", err)
		verifyProof(t, proof, root)

		// Verify each value.
		for i, key := range c.keys {
			err := proof.VerifyItem(i, []byte{key}, []byte{key})
			require.NoError(err)
		}
	}
}

func verifyProof(t *testing.T, proof *RangeProof, root []byte) {
	// Proof must verify.
	require.NoError(t, proof.Verify(root))

	// Write/Read then verify.
	cdc := amino.NewCodec()
	proofBytes := cdc.MustMarshalBinary(proof)
	var proof2 = new(RangeProof)
	err := cdc.UnmarshalBinary(proofBytes, proof2)
	require.Nil(t, err, "Failed to read KeyExistsProof from bytes: %v", err)

	// Random mutations must not verify
	for i := 0; i < 1e4; i++ {
		badProofBytes := test.MutateByteSlice(proofBytes)
		var badProof = new(RangeProof)
		err := cdc.UnmarshalBinary(badProofBytes, badProof)
		if err != nil {
			continue // couldn't even decode.
		}
		// re-encode to make sure it's actually different.
		badProofBytes2 := cdc.MustMarshalBinary(badProof)
		if bytes.Equal(proofBytes, badProofBytes2) {
			continue // didn't mutate successfully.
		}
		// may be invalid... errors are okay
		if err == nil {
			assert.Error(t, badProof.Verify(root),
				"Proof was still valid after a random mutation:\n%X\n%X",
				proofBytes, badProofBytes)
		}
	}

	// targetted changes fails...
	proof.RootHash = test.MutateByteSlice(proof.RootHash)
	assert.Error(t, proof.Verify(root))
}

//----------------------------------------

func flatten(bzz [][]byte) (res []byte) {
	for _, bz := range bzz {
		res = append(res, bz...)
	}
	return res
}
