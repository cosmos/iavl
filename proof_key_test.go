package iavl

import (
	"testing"

	"github.com/stretchr/testify/require"

	cmn "github.com/tendermint/tmlibs/common"
)

func TestSerializeProofs(t *testing.T) {
	require := require.New(t)

	tree := NewTree(nil, 0)
	for _, ikey := range []byte{0x17, 0x42, 0x99} {
		key := []byte{ikey}
		tree.Set(key, cmn.RandBytes(8))
	}
	root := tree.Hash()

	// test with key exists
	key := []byte{0x17}
	val, proof, err := tree.GetWithProof(key)
	require.Nil(err, "%+v", err)
	require.NotNil(val)

	bin := proof.Bytes()
	proof2, err := ReadKeyProof(bin)
	require.Nil(err, "%+v", err)
	require.NoError(proof2.Verify(key, val, root))

	_, ok := proof2.(*KeyExistsProof)
	require.True(ok, "Proof should be *KeyExistsProof")

	// test with key absent
	key = []byte{0x38}
	val, proof, err = tree.GetWithProof(key)
	require.Nil(err, "%+v", err)
	require.Nil(val)
	bin = proof.Bytes()
	aproof, err := ReadKeyProof(bin)
	require.Nil(err, "%+v", err)
	require.NoError(aproof.Verify(key, val, root))
}
