package iavl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// decodeBytes decodes a varint length-prefixed byte slice, returning it along with the number
// of input bytes read.
func decodeBytes(bz []byte) ([]byte, int, error) {
	size, n, err := decodeUvarint(bz)
	if err != nil {
		return nil, n, err
	}
	if int(size) < 0 {
		return nil, n, fmt.Errorf("invalid negative length %v decoding []byte", size)
	}
	if len(bz) < n+int(size) {
		return nil, n, fmt.Errorf("insufficient bytes decoding []byte of length %v", size)
	}
	bz2 := make([]byte, size)
	copy(bz2, bz[n:n+int(size)])
	n += int(size)
	return bz2, n, nil
}

// decodeUvarint decodes a varint-encoded unsigned integer from a byte slice, returning it and the
// number of bytes decoded.
func decodeUvarint(bz []byte) (uint64, int, error) {
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

// decodeVarint decodes a varint-encoded integer from a byte slice, returning it and the number of
// bytes decoded.
func decodeVarint(bz []byte) (int64, int, error) {
	i, n := binary.Varint(bz)
	if n == 0 {
		return i, n, errors.New("buffer too small")
	} else if n < 0 {
		// value larger than 64 bits (overflow)
		// and -n is the number of bytes read
		n = -n
		return i, n, errors.New("EOF decoding varint")
	}
	return i, n, nil
}

// encodeBytes writes a varint length-prefixed byte slice to the writer.
func encodeBytes(w io.Writer, bz []byte) error {
	err := encodeUvarint(w, uint64(len(bz)))
	if err != nil {
		return err
	}
	_, err = w.Write(bz)
	return err
}

// encodeUvarint writes a varint-encoded unsigned integer to an io.Writer.
func encodeUvarint(w io.Writer, u uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], u)
	_, err := w.Write(buf[0:n])
	return err
}

// encodeVarint writes a varint-encoded integer to an io.Writer.
func encodeVarint(w io.Writer, i int64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], i)
	_, err := w.Write(buf[0:n])
	return err
}
