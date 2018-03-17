package iavl

import (
	"bytes"
	"fmt"

	cmn "github.com/tendermint/tmlibs/common"
)

// KeyProof represents a proof of existence or absence of a single key.
type KeyProof interface {
	// Verify verfies the proof is valid. To verify absence,
	// the value should be nil.
	Verify(key, value, root []byte) cmn.Error

	// Root returns the root hash of the proof.
	Root() []byte

	// Serialize itself
	Bytes() []byte
}

const (
	// Used for serialization of proofs.
	keyExistsMagicNumber = 0x50
	keyAbsentMagicNumber = 0x51
)

// KeyExistsProof represents a proof of existence of a single key.
type KeyExistsProof struct {
	RootHash cmn.HexBytes `json:"root_hash"`
	Version  int64        `json:"version"`

	*PathToKey `json:"path"`
}

func (proof *KeyExistsProof) Root() []byte {
	return proof.RootHash
}

// Verify verifies the proof is valid and returns an error if it isn't.
func (proof *KeyExistsProof) Verify(key []byte, value []byte, root []byte) cmn.Error {
	if !bytes.Equal(proof.RootHash, root) {
		return cmn.NewErrorWithType(ErrInvalidRoot, "")
	}
	if key == nil || value == nil {
		return cmn.NewErrorWithType(ErrInvalidInputs, "")
	}
	return proof.PathToKey.verify(proofLeafNode{key, value, proof.Version}.Hash(), root)
}

// Bytes returns a go-amino binary serialization
func (proof *KeyExistsProof) Bytes() (res []byte) {
	bz, err := cdc.MarshalBinary(proof)
	if err != nil {
		panic(fmt.Sprintf("error marshaling proof (%v): %v", proof, err))
	}
	res = append([]byte{keyExistsMagicNumber}, bz...)
	return
}

// readKeyExistsProof will deserialize a KeyExistsProof from bytes.
func readKeyExistsProof(data []byte) (*KeyExistsProof, cmn.Error) {
	proof := new(KeyExistsProof)
	err := cdc.UnmarshalBinary(data, proof)
	if err != nil {
		return nil, cmn.NewErrorWithCause(err, "unmarshalling proof")
	} else {
		return proof, nil
	}
}

///////////////////////////////////////////////////////////////////////////////

// KeyAbsentProof represents a proof of the absence of a single key.
type KeyAbsentProof struct {
	RootHash cmn.HexBytes `json:"root_hash"`

	Left  *pathWithNode `json:"left"`
	Right *pathWithNode `json:"right"`
}

func (proof *KeyAbsentProof) Root() []byte {
	return proof.RootHash
}

func (p *KeyAbsentProof) String() string {
	return fmt.Sprintf("KeyAbsentProof\nroot=%s\nleft=%s%#v\nright=%s%#v\n", p.RootHash, p.Left.Path, p.Left.Node, p.Right.Path, p.Right.Node)
}

// Verify verifies the proof is valid and returns an error if it isn't.
func (proof *KeyAbsentProof) Verify(key, value []byte, root []byte) cmn.Error {
	if !bytes.Equal(proof.RootHash, root) {
		return cmn.NewErrorWithType(ErrInvalidRoot, "")
	}
	if key == nil || value != nil {
		return cmn.NewErrorWithType(ErrInvalidInputs, "")
	}

	if proof.Left == nil && proof.Right == nil {
		return cmn.NewErrorWithType(ErrInvalidProof, "")
	}
	err := verifyPaths(proof.Left, proof.Right, key, key, root)
	if err != nil {
		return err.Trace("verifying path")
	}

	err = verifyKeyAbsence(proof.Left, proof.Right)
	if err != nil {
		return err.Trace("verifying key absence")
	}

	return nil
}

// Bytes returns a go-amino binary serialization
func (proof *KeyAbsentProof) Bytes() (res []byte) {
	bz, err := cdc.MarshalBinary(proof)
	if err != nil {
		panic(fmt.Sprintf("error marshaling proof (%v): %v", proof, err))
	}
	res = append([]byte{keyAbsentMagicNumber}, bz...)
	return
}

// readKeyAbsentProof will deserialize a KeyAbsentProof from bytes.
func readKeyAbsentProof(data []byte) (*KeyAbsentProof, cmn.Error) {
	proof := new(KeyAbsentProof)
	err := cdc.UnmarshalBinary(data, proof)
	if err != nil {
		return nil, cmn.NewErrorWithCause(err, "unmarshalling proof")
	} else {
		return proof, nil
	}
}

// ReadKeyProof reads a KeyProof from a byte-slice.
func ReadKeyProof(data []byte) (KeyProof, cmn.Error) {
	if len(data) == 0 {
		return nil, cmn.NewError("Proof bytes are empty")
	}
	b, val := data[0], data[1:]

	switch b {
	case keyExistsMagicNumber:
		return readKeyExistsProof(val)
	case keyAbsentMagicNumber:
		return readKeyAbsentProof(val)
	}
	return nil, cmn.NewError("Unrecognized proof type")
}

///////////////////////////////////////////////////////////////////////////////

// InnerKeyProof represents a proof of existence of an inner node key.
type InnerKeyProof struct {
	*KeyExistsProof
}

// Verify verifies the proof is valid and returns an error if it isn't.
func (proof *InnerKeyProof) Verify(hash []byte, value []byte, root []byte) cmn.Error {
	if !bytes.Equal(proof.RootHash, root) {
		return cmn.NewErrorWithType(ErrInvalidRoot, "")
	}
	if hash == nil || value != nil {
		return cmn.NewErrorWithType(ErrInvalidInputs, "")
	}
	return proof.PathToKey.verify(hash, root)
}

// ReadKeyInnerProof will deserialize a InnerKeyProof from bytes.
func ReadInnerKeyProof(data []byte) (*InnerKeyProof, cmn.Error) {
	proof := new(InnerKeyProof)
	err := cdc.UnmarshalBinary(data, proof)
	if err != nil {
		return nil, cmn.NewErrorWithCause(err, "unmarshalling proof")
	} else {
		return proof, nil
	}
}
