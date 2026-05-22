package encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"sync"
)

const hash32ByteLength = 32

// bufPool provides temporary buffers to reduce allocations.
var bufPool = &sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// varintPool provides temporary slices for varint encoding.
var varintPool = &sync.Pool{
	New: func() interface{} {
		return &[binary.MaxVarintLen64]byte{}
	},
}

// uvarintPool provides temporary slices for uvarint encoding.
var uvarintPool = &sync.Pool{
	New: func() interface{} {
		return &[binary.MaxVarintLen64]byte{}
	},
}

func writeBytes(w io.Writer, b []byte) error {
	_, err := w.Write(b)
	return err
}

// DecodeBytes decodes a varint length-prefixed byte slice, returning it along with the number
// of input bytes read. Assumes bz will not be mutated.
func DecodeBytes(bz []byte) ([]byte, int, error) {
	s, n, err := DecodeUvarint(bz)
	if err != nil {
		return nil, n, err
	}
	size := int(s)
	// Check for overflow or negative sizes.
	if s >= uint64(^uint(0)>>1) || size < 0 {
		return nil, n, fmt.Errorf("invalid out of range length %v decoding []byte", s)
	}
	end := n + size
	// Check for overflow of end index.
	if end < n {
		return nil, n, fmt.Errorf("invalid out of range length %v decoding []byte", size)
	}
	if len(bz) < end {
		return nil, n, fmt.Errorf("insufficient bytes decoding []byte of length %v", size)
	}
	return bz[n:end], end, nil
}

// DecodeUvarint decodes a varint-encoded unsigned integer from a byte slice.
func DecodeUvarint(bz []byte) (uint64, int, error) {
	u, n := binary.Uvarint(bz)
	if n == 0 {
		// buf too small
		return u, n, errors.New("buffer too small")
	} else if n < 0 {
		// value larger than 64 bits (overflow)
		// and -n is the number of bytes read
		n = -n
		return u, n, errors.New("EOF decoding uvarint")
	}
	return u, n, nil
}

// DecodeVarint decodes a varint-encoded integer from a byte slice.
func DecodeVarint(bz []byte) (int64, int, error) {
	i, n := binary.Varint(bz)
	if n == 0 {
		return i, n, errors.New("buffer too small")
	} else if n < 0 {
		n = -n
		return i, n, errors.New("EOF decoding varint")
	}
	return i, n, nil
}

// EncodeBytes writes a varint length-prefixed byte slice to the writer.
func EncodeBytes(w io.Writer, bz []byte) error {
	if err := EncodeUvarint(w, uint64(len(bz))); err != nil {
		return err
	}
	return writeBytes(w, bz)
}

// Encode32BytesHash writes a hardcoded 1-byte length prefix (32) and then a 32-byte hash.
func Encode32BytesHash(w io.Writer, bz []byte) error {
	if len(bz) != hash32ByteLength {
		return fmt.Errorf("expected %d-byte hash, got %d bytes", hash32ByteLength, len(bz))
	}
	if err := writeBytes(w, []byte{hash32ByteLength}); err != nil {
		return err
	}
	return writeBytes(w, bz)
}

// Encode32BytesHashSlice returns a length-prefixed 32-byte hash as a slice.
func Encode32BytesHashSlice(bz []byte) ([]byte, error) {
	if len(bz) != hash32ByteLength {
		return nil, fmt.Errorf("expected %d-byte hash, got %d bytes", hash32ByteLength, len(bz))
	}
	out := make([]byte, 1+hash32ByteLength)
	out[0] = hash32ByteLength
	copy(out[1:], bz)
	return out, nil
}

// EncodeBytesSlice length-prefixes the byte slice and returns it.
func EncodeBytesSlice(bz []byte) ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	err := EncodeBytes(buf, bz)
	bytesCopy := make([]byte, buf.Len())
	copy(bytesCopy, buf.Bytes())
	return bytesCopy, err
}

// EncodeBytesSize returns the byte size of the given slice including length-prefixing.
func EncodeBytesSize(bz []byte) int {
	return EncodeUvarintSize(uint64(len(bz))) + len(bz)
}

// EncodeUvarint writes a varint-encoded unsigned integer to an io.Writer.
func EncodeUvarint(w io.Writer, u uint64) error {
	buf := uvarintPool.Get().(*[binary.MaxVarintLen64]byte)
	n := binary.PutUvarint(buf[:], u)
	_, err := w.Write(buf[:n])
	uvarintPool.Put(buf)
	return err
}

// EncodeUvarintSize returns the byte size of the given integer as a varint.
func EncodeUvarintSize(u uint64) int {
	if u == 0 {
		return 1
	}
	return (bits.Len64(u) + 6) / 7
}

// EncodeVarint writes a varint-encoded integer to an io.Writer.
func EncodeVarint(w io.Writer, i int64) error {
	if bw, ok := w.(io.ByteWriter); ok {
		return fVarintEncode(bw, i)
	}
	buf := varintPool.Get().(*[binary.MaxVarintLen64]byte)
	n := binary.PutVarint(buf[:], i)
	_, err := w.Write(buf[:n])
	varintPool.Put(buf)
	return err
}

func fVarintEncode(bw io.ByteWriter, x int64) error {
	// Firstly convert it into a uvarint
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	for ux >= 0x80 {
		// Convert it into a byte then toggle the
		// 7th bit to indicate that more bytes coming.
		// byte(x & 0x7f) is redundant but useful for illustrative
		// purposes when translating to other languages
		if err := bw.WriteByte(byte(ux&0x7f) | 0x80); err != nil {
			return err
		}
		ux >>= 7
	}
	return bw.WriteByte(byte(ux & 0x7f))
}

// EncodeVarintSize returns the byte size of the given integer as a varint.
func EncodeVarintSize(i int64) int {
	ux := uint64(i) << 1
	if i < 0 {
		ux = ^ux
	}
	return EncodeUvarintSize(ux)
}
