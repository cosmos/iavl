package iavl

import (
	"fmt"

	cmn "github.com/tendermint/tmlibs/common"
)

const ProofOpIAVLValue = "iavl:v"

// IAVLValueOp takes a key and a single value as argument and
// produces the root hash.
//
// If the produced root hash matches the expected hash, the proof
// is good.
type IAVLValueOp struct {
	// encoded in ProofOp.Key, not .Data
	key string

	// To encode in ProofOp.Data
	Proof *RangeProof `json:"proof"`
}

var _ ProofOperator = IAVLValueOp{}

func NewIAVLValueOp(key string, proof *RangeProof) IAVLValueOp {
	return IAVLValueOp{
		key:   key,
		Proof: proof,
	}
}

func (op IAVLValueOp) String() string {
	return fmt.Sprintf("IAVLValueOp{%v}", op.GetKey())
}

func (op IAVLValueOp) Run(args [][]byte) ([][]byte, error) {
	if len(args) != 1 {
		return nil, cmn.NewError("Value size is not 1")
	}
	value := args[0]

	// Compute the root hash and assume it is valid.
	// The caller checks the ultimate root later.
	root := op.Proof.ComputeRootHash()
	err := op.Proof.Verify(root)
	if err != nil {
		return nil, cmn.ErrorWrap(err, "computing root hash")
	}
	err = op.Proof.VerifyItem(op.key, op.value)
	if err != nil {
		return nil, cmn.ErrorWrap(err, "verifying absence")
	}
	return [][]byte{root}, nil
}

func (op IAVLValueOp) GetKey() string {
	return op.key
}

func (op IAVLValueOp) ProofOp() ProofOp {
	bz := cdc.MustMarshalBinary(op)
	return ProofOp{
		Type: ProofOpIAVLValue,
		Key:  op.key,
		Data: bz,
	}, nil
}
