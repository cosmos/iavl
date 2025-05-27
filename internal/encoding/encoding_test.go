package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

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

			b, n, err := DecodeBytes(buf)
			if tc.expectErr {
				require.Error(t, err)
				require.Equal(t, varintBytes, n)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, varintBytes+int(tc.lengthPrefix), n)
				require.Equal(t, tc.bz[:tc.lengthPrefix], b)
			}
		})
	}
}

func TestDecodeBytes_invalidVarint(t *testing.T) {
	_, _, err := DecodeBytes([]byte{0xff})
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
			err := Encode32BytesHash(&buf, tc.input)

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
			out, err := Encode32BytesHashSlice(tc.input)

			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedOut, out)
			}
		})
	}
}

func TestHandleVarintDecode(t *testing.T) {
	tests := []struct {
		n       int
		what    string
		wantErr bool
	}{
		{0, "test", true},
		{-2, "test", true},
		{5, "test", false},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("n=%d", tc.n), func(t *testing.T) {
			err := handleVarintDecode(tc.n, tc.what)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
