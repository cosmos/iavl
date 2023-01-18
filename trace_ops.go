package iavl

import (
	ics23 "github.com/confio/ics23/go"
)

const (
	WriteOp  Operation = "write"
	ReadOp   Operation = "read"
	DeleteOp Operation = "delete"
)

type (
	// operation represents an tree operation
	Operation string
)

// Witness data represents a trace operation along with inclusion proofs required for said operation
type WitnessData struct {
	Operation Operation
	Key       []byte
	Value     []byte
	Proofs    []*ics23.ExistenceProof
}
