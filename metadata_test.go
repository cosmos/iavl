package iavl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVersionMetadata_Serialize(t *testing.T) {
	vm := &VersionMetadata{
		Version:   14,
		Committed: time.Now().UTC().Unix(),
		Updated:   time.Now().UTC().Unix(),
		RootHash:  []byte{0x04, 0x05, 0x00, 0xff, 0x04, 0x05, 0x00, 0xff},
		Snapshot:  true,
	}

	bz, err := vm.marshal()
	require.NoError(t, err)
	require.NotNil(t, bz)

	vm2, err := unmarshalVersionMetadata(bz)
	require.NoError(t, err)
	require.Equal(t, vm.String(), vm2.String())
}
