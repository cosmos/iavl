package iavl

import (
	"fmt"

	cmn "github.com/tendermint/tmlibs/common"
)

const ProofOpIAVLAbsence = "iavl:a"

// IAVLAbsenceOp takes a key as its only argument
//
// If the produced root hash matches the expected hash, the proof
// is good.
type IAVLAbsenceOp struct {
	// encoded in ProofOp.Key, not .Data
	key string

	// To encode in ProofOp.Data
	Proof *RangeProof `json:"proof"`
}

var _ ProofOperator = IAVLAbsenceOp{}

func NewIAVLAbsenceOp(key string, proof *RangeProof) IAVLAbsenceOp {
	return IAVLAbsenceOp{
		key:   key,
		Proof: proof,
	}
}

func (op IAVLAbsenceOp) String() string {
	return fmt.Sprintf("IAVLAbsenceOp{%v}", op.GetKey())
}

func (op IAVLAbsenceOp) Run(args [][]byte) ([][]byte, error) {
	if len(args) != 0 {
		return nil, cmn.NewError("expected 0 args, got %v", len(args))
	}
	// Compute the root hash and assume it is valid.
	// The caller checks the ultimate root later.
	root := op.Proof.ComputeRootHash()
	err := op.Proof.Verify(root)
	if err != nil {
		return nil, cmn.ErrorWrap(err, "computing root hash")
	}
	err = op.Proof.VerifyAbsence(op.key)
	if err != nil {
		return nil, cmn.ErrorWrap(err, "verifying absence")
	}
	return [][]byte{root}, nil
}

func (op IAVLAbsenceOp) GetKey() string {
	return op.key
}

func (op IAVLAbsenceOp) ProofOp() ProofOp {
	bz := cdc.MustMarshalBinary(op)
	return ProofOp{
		Type: ProofOpIAVLAbsence,
		Key:  op.key,
		Data: bz,
	}, nil
}
