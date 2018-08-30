package iavl

import (
	"encoding/binary"
	"fmt"
)

// Provides a fixed-width lexicographically sortable []byte key format
type KeyFormat struct {
	prefix byte
	layout []int
	length int
}

// Create a []byte key format based on a single byte prefix and fixed width key segments each of whose length is
// specified by by the corresponding element of layout
func NewKeyFormat(prefix byte, layout ...int) *KeyFormat {
	// For prefix
	length := 1
	for _, l := range layout {
		length += int(l)
	}
	return &KeyFormat{
		prefix: prefix,
		layout: layout,
		length: length,
	}
}

// Format the byte segments into the key format - will panic if the segment lengths to do match the layout.
func (kf *KeyFormat) KeyBytes(segments ...[]byte) []byte {
	key := make([]byte, kf.length)
	key[0] = kf.prefix
	n := 1
	for i, s := range segments {
		l := kf.layout[i]
		if len(s) > l {
			panic(fmt.Errorf("length of segment %X provided to KeyFormat.KeyBytes() is longer than the %d bytes "+
				"required by layout for segment %d", s, l, i))
		}
		n += l
		// Big endian so pad on left if not given the full width for this segment
		copy(key[n-len(s):n], s)
	}
	return key[:n]
}

// Format the args passed into the key format - will panic if the arguments passed to not match the length
// of the segment to which they correspond. When called with no arguments returns the raw prefix (useful as a start
// element of the entire keys space when sorted lexicographically)
func (kf *KeyFormat) Key(args ...interface{}) []byte {
	if len(args) > len(kf.layout) {
		panic(fmt.Errorf("KeyFormat.Key() is provided with %d args but format only has %d segments",
			len(args), len(kf.layout)))
	}
	segments := make([][]byte, len(args))
	for i, a := range args {
		segments[i] = format(a)
	}
	return kf.KeyBytes(segments...)
}

// Reads out the bytes associated with each segment of the key format from key
func (kf *KeyFormat) ScanBytes(key []byte) [][]byte {
	segments := make([][]byte, len(kf.layout))
	n := 1
	for i, l := range kf.layout {
		n += l
		if n > len(key) {
			return segments[:i]
		}
		segments[i] = key[n-l : n]
	}
	return segments
}

// Extracts the segments into the values pointed to by each of args. Each arg must be a pointer to int64, uint64, or
// []byte, and the width of the args must match layout.
func (kf *KeyFormat) Scan(key []byte, args ...interface{}) {
	segments := kf.ScanBytes(key)
	if len(args) > len(segments) {
		panic(fmt.Errorf("KeyFormat.Scan() is provided with %d args but format only has %d segments in key %X",
			len(args), len(segments), key))
	}
	for i, a := range args {
		scan(a, segments[i])
	}
}

// Return the prefix as a string
func (kf *KeyFormat) Prefix() string {
	return string([]byte{kf.prefix})
}

func scan(a interface{}, value []byte) {
	switch v := a.(type) {
	case *int64:
		*v = int64(binary.BigEndian.Uint64(value))
	case *uint64:
		*v = binary.BigEndian.Uint64(value)
	case *[]byte:
		*v = value
	default:
		panic(fmt.Errorf("KeyFormat scan() does not support scanning value of type %T: %v", a, a))
	}
}

func format(a interface{}) []byte {
	switch v := a.(type) {
	case uint64:
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, v)
		return bs
	case uint:
		return format(uint64(v))
	case int64:
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(v))
		return bs
	case int:
		return format(int64(v))
	case []byte:
		return v
	default:
		panic(fmt.Errorf("KeyFormat format() does not support formatting value of type %T: %v", a, a))
	}
}
