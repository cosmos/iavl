package iavl

import (
	"bytes"
	"fmt"

	"github.com/tendermint/go-wire"
	"github.com/tendermint/go-wire/data"
)

// KeyProof represents a proof of existence or absence of a single key.
type KeyProof interface {
	// Verify verfies the proof is valid. To verify absence,
	// the value should be nil.
	Verify(key, value, root []byte) error

	// Root returns the root hash of the proof.
	Root() []byte
}

// KeyExistsProof represents a proof of existence of a single key.
type KeyExistsProof struct {
	RootHash data.Bytes `json:"root_hash"`
	Version  uint64     `json:"version"`

	*PathToKey `json:"path"`
}

func (proof *KeyExistsProof) Root() []byte {
	return proof.RootHash
}

// Verify verifies the proof is valid and returns an error if it isn't.
func (proof *KeyExistsProof) Verify(key []byte, value []byte, root []byte) error {
	if !bytes.Equal(proof.RootHash, root) {
		return ErrInvalidRoot
	}
	if key == nil || value == nil {
		return ErrInvalidInputs
	}
	return proof.PathToKey.verify(IAVLProofLeafNode{key, value}, root, proof.Version)
}

// ReadKeyExistsProof will deserialize a KeyExistsProof from bytes.
func ReadKeyExistsProof(data []byte) (*KeyExistsProof, error) {
	// TODO: make go-wire never panic
	n, err := int(0), error(nil)
	proof := wire.ReadBinary(&KeyExistsProof{}, bytes.NewBuffer(data), proofLimit, &n, &err).(*KeyExistsProof)
	return proof, err
}

// KeyAbsentProof represents a proof of the absence of a single key.
type KeyAbsentProof struct {
	RootHash data.Bytes `json:"root_hash"`
	Version  uint64     `json:"version"`

	Left  *PathWithNode `json:"left"`
	Right *PathWithNode `json:"right"`
}

func (proof *KeyAbsentProof) Root() []byte {
	return proof.RootHash
}

func (p *KeyAbsentProof) String() string {
	return fmt.Sprintf("KeyAbsentProof\nroot=%s\nleft=%s%#v\nright=%s%#v\n", p.RootHash, p.Left.Path, p.Left.Node, p.Right.Path, p.Right.Node)
}

// Verify verifies the proof is valid and returns an error if it isn't.
func (proof *KeyAbsentProof) Verify(key, value []byte, root []byte) error {
	if !bytes.Equal(proof.RootHash, root) {
		return ErrInvalidRoot
	}
	if key == nil || value != nil {
		return ErrInvalidInputs
	}

	if proof.Left == nil && proof.Right == nil {
		return ErrInvalidProof()
	}
	if err := verifyPaths(proof.Left, proof.Right, key, key, root, proof.Version); err != nil {
		return err
	}

	return verifyKeyAbsence(proof.Left, proof.Right)
}
