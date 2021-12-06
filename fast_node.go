package iavl

import (
	"github.com/pkg/errors"
	"io"
)

// NOTE: This file favors int64 as opposed to int for size/counts.
// The Tree on the other hand favors int.  This is intentional.

type FastNode struct {
	key                  []byte
	versionLastUpdatedAt int64
	value                []byte
	// leafHash             []byte // TODO: Look into if this would help with proof stuff.
}

// NewFastNode returns a new fast node from a value and version.
func NewFastNode(key []byte, value []byte, version int64) *FastNode {
	return &FastNode{
		key:                  key,
		versionLastUpdatedAt: version,
		value:                value,
	}
}

// DeserializeFastNode constructs an *FastNode from an encoded byte slice.
func DeserializeFastNode(buf []byte) (*FastNode, error) {
	val, n, cause := decodeBytes(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding fastnode.value")
	}
	buf = buf[n:]

	ver, _, cause := decodeVarint(buf)
	if cause != nil {
		return nil, errors.Wrap(cause, "decoding fastnode.version")
	}

	fastNode := &FastNode{
		versionLastUpdatedAt: ver,
		value:                val,
	}

	return fastNode, nil
}

func (node *FastNode) encodedSize() int {
	n := 1 +
		encodeBytesSize(node.key) +
		encodeVarintSize(node.versionLastUpdatedAt) +
		encodeBytesSize(node.value)
	return n
}

// writeBytes writes the FastNode as a serialized byte slice to the supplied io.Writer.
func (node *FastNode) writeBytes(w io.Writer) error {
	if node == nil {
		return errors.New("cannot write nil node")
	}
	cause := encodeBytes(w, node.key)
	if cause != nil {
		return errors.Wrap(cause, "writing key")
	}
	cause = encodeVarint(w, node.versionLastUpdatedAt)
	if cause != nil {
		return errors.Wrap(cause, "writing version last updated at")
	}
	cause = encodeBytes(w, node.value)
	if cause != nil {
		return errors.Wrap(cause, "writing value")
	}
	return nil
}
