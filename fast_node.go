package iavl

import (
	"github.com/pkg/errors"
)

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

type FastNode struct {
	key                  []byte
	versionLastUpdatedAt int64
	value                []byte
	leafHash             []byte // TODO: Look into if this would help with proof stuff.
}

// NewFastNode returns a new fast node from a value and version.
func NewFastNode(value []byte, version int64) *FastNode {
	return &FastNode{
		versionLastUpdatedAt: version,
		value:                value,
	}
}

// MakeFastNode constructs an *FastNode from an encoded byte slice.
func MakeFastNode(buf []byte) (*FastNode, error) {
	val, n, cause := decodeBytes(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding fastnode.value")
	}
	buf = buf[n:]

	ver, _, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding fastnode.version")
	}

	fastNode := NewFastNode(val, ver)

	return fastNode, nil
}
