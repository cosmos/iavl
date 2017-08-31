package iavl

import (
	"bytes"
	"fmt"

	"golang.org/x/crypto/ripemd160"

	"github.com/tendermint/go-wire"
	"github.com/tendermint/go-wire/data"
	. "github.com/tendermint/tmlibs/common"
)

const proofLimit = 1 << 16 // 64 KB

type IAVLProof struct {
	LeafHash   []byte
	InnerNodes []IAVLProofInnerNode
	RootHash   []byte
}

func (proof *IAVLProof) Verify(key []byte, value []byte, root []byte) bool {
	if !bytes.Equal(proof.RootHash, root) {
		return false
	}
	leafNode := IAVLProofLeafNode{KeyBytes: key, ValueBytes: value}
	leafHash := leafNode.Hash()
	if !bytes.Equal(leafHash, proof.LeafHash) {
		return false
	}
	hash := leafHash
	for _, branch := range proof.InnerNodes {
		hash = branch.Hash(hash)
	}
	return bytes.Equal(proof.RootHash, hash)
}

// Please leave this here!  I use it in light-client to fulfill an interface
func (proof *IAVLProof) Root() []byte {
	return proof.RootHash
}

// ReadKeyExistsProof will deserialize a KeyExistsProof from bytes.
func ReadKeyExistsProof(data []byte) (*KeyExistsProof, error) {
	// TODO: make go-wire never panic
	n, err := int(0), error(nil)
	proof := wire.ReadBinary(&KeyExistsProof{}, bytes.NewBuffer(data), proofLimit, &n, &err).(*KeyExistsProof)
	return proof, err
}

type IAVLProofInnerNode struct {
	Height int8
	Size   int
	Left   []byte
	Right  []byte
}

func (n *IAVLProofInnerNode) String() string {
	return fmt.Sprintf("IAVLProofInnerNode[height=%d, %x / %x]", n.Height, n.Left, n.Right)
}

func (branch IAVLProofInnerNode) Hash(childHash []byte) []byte {
	hasher := ripemd160.New()
	buf := new(bytes.Buffer)
	n, err := int(0), error(nil)
	wire.WriteInt8(branch.Height, buf, &n, &err)
	wire.WriteVarint(branch.Size, buf, &n, &err)
	if len(branch.Left) == 0 {
		wire.WriteByteSlice(childHash, buf, &n, &err)
		wire.WriteByteSlice(branch.Right, buf, &n, &err)
	} else {
		wire.WriteByteSlice(branch.Left, buf, &n, &err)
		wire.WriteByteSlice(childHash, buf, &n, &err)
	}
	if err != nil {
		PanicCrisis(Fmt("Failed to hash IAVLProofInnerNode: %v", err))
	}
	// fmt.Printf("InnerNode hash bytes: %X\n", buf.Bytes())
	hasher.Write(buf.Bytes())
	return hasher.Sum(nil)
}

type IAVLProofLeafNode struct {
	KeyBytes   data.Bytes `json:"key"`
	ValueBytes data.Bytes `json:"value"`
}

func (leaf IAVLProofLeafNode) Hash() []byte {
	hasher := ripemd160.New()
	buf := new(bytes.Buffer)
	n, err := int(0), error(nil)
	wire.WriteInt8(0, buf, &n, &err)
	wire.WriteVarint(1, buf, &n, &err)
	wire.WriteByteSlice(leaf.KeyBytes, buf, &n, &err)
	wire.WriteByteSlice(leaf.ValueBytes, buf, &n, &err)
	if err != nil {
		PanicCrisis(Fmt("Failed to hash IAVLProofLeafNode: %v", err))
	}
	// fmt.Printf("LeafNode hash bytes:   %X\n", buf.Bytes())
	hasher.Write(buf.Bytes())
	return hasher.Sum(nil)
}

func (leaf IAVLProofLeafNode) isLesserThan(key []byte) bool {
	return bytes.Compare(leaf.KeyBytes, key) == -1
}

func (leaf IAVLProofLeafNode) isGreaterThan(key []byte) bool {
	return bytes.Compare(leaf.KeyBytes, key) == 1
}

