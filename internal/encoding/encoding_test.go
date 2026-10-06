package encoding_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/cosmos/iavl/internal/encoding"
	"github.com/stretchr/testify/require"
)

func TestDecodeBytes(t *testing.T) {
	bz := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	testcases := map[string]struct {
		bz           []byte
		lengthPrefix uint64
		expect       []byte
		expectErr    bool
	}{
		"full":                      {bz, 8, bz, false},
		"empty":                     {bz, 0, []byte{}, false},
		"partial":                   {bz, 3, []byte{0, 1, 2}, false},
		"out of bounds":             {bz, 9, nil, true},
		"empty input":               {[]byte{}, 0, []byte{}, false},
		"empty input out of bounds": {[]byte{}, 1, nil, true},
		"max int32":                 {bz, uint64(math.MaxInt32), nil, true},
		"max int32 +10":             {bz, uint64(math.MaxInt32) + 10, nil, true},
		"max uint64":                {bz, uint64(math.MaxUint64), nil, true},
	}

	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			buf := make([]byte, binary.MaxVarintLen64)
			varintBytes := binary.PutUvarint(buf, tc.lengthPrefix)
			buf = append(buf[:varintBytes], tc.bz...)

			b, n, err := encoding.DecodeBytes(buf)
			if tc.expectErr {
				require.Error(t, err)
				require.Equal(t, varintBytes, n)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, varintBytes+int(tc.lengthPrefix), n) // nolint:gosec // testing code
				require.Equal(t, tc.bz[:tc.lengthPrefix], b)
			}
		})
	}
}

func TestDecodeBytes_invalidVarint(t *testing.T) {
	_, _, err := encoding.DecodeBytes([]byte{0xff})
	require.Error(t, err)
}

func TestEncode32BytesHash(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expectErr   bool
		expectedOut []byte
	}{
		{
			name:        "valid 32-byte hash",
			input:       bytes.Repeat([]byte{0xAB}, 32),
			expectErr:   false,
			expectedOut: append([]byte{0x20}, bytes.Repeat([]byte{0xAB}, 32)...),
		},
		{
			name:      "too short (31 bytes)",
			input:     bytes.Repeat([]byte{0xAB}, 31),
			expectErr: true,
		},
		{
			name:      "too long (33 bytes)",
			input:     bytes.Repeat([]byte{0xAB}, 33),
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := encoding.Encode32BytesHash(&buf, tc.input)

			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedOut, buf.Bytes())
			}
		})
	}
}

func TestEncode32BytesHashSlice(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expectErr   bool
		expectedOut []byte
	}{
		{
			name:        "valid 32-byte hash",
			input:       bytes.Repeat([]byte{0x01}, 32),
			expectErr:   false,
			expectedOut: append([]byte{0x20}, bytes.Repeat([]byte{0x01}, 32)...),
		},
		{
			name:      "invalid (length 30)",
			input:     bytes.Repeat([]byte{0x02}, 30),
			expectErr: true,
		},
		{
			name:      "invalid (length 40)",
			input:     bytes.Repeat([]byte{0x03}, 40),
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := encoding.Encode32BytesHashSlice(tc.input)

			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedOut, out)
			}
		})
	}
}

func TestEncodeBytesSliceAndSize(t *testing.T) {
	input := []byte("hello world")
	out, err := encoding.EncodeBytesSlice(input)
	require.NoError(t, err)

	size := encoding.EncodeBytesSize(input)
	require.Equal(t, len(out), size)
}

func TestEncodeUvarintAndSize(t *testing.T) {
	var buf bytes.Buffer
	u := uint64(123456)
	err := encoding.EncodeUvarint(&buf, u)
	require.NoError(t, err)

	expectedSize := encoding.EncodeUvarintSize(u)
	require.Equal(t, expectedSize, buf.Len())
}

func TestEncodeVarintAndSize(t *testing.T) {
	var buf bytes.Buffer
	i := int64(-78910)
	err := encoding.EncodeVarint(&buf, i)
	require.NoError(t, err)

	expectedSize := encoding.EncodeVarintSize(i)
	require.Equal(t, expectedSize, buf.Len())
}

func TestDecodeUvarint(t *testing.T) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, 1000)
	val, read, err := encoding.DecodeUvarint(buf[:n])
	require.NoError(t, err)
	require.Equal(t, uint64(1000), val)
	require.Equal(t, n, read)
}

func TestDecodeVarint(t *testing.T) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, -12345)
	val, read, err := encoding.DecodeVarint(buf[:n])
	require.NoError(t, err)
	require.Equal(t, int64(-12345), val)
	require.Equal(t, n, read)
}
