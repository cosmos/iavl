package iavl_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/iavl"
)

func TestVersionMetadata_Serialize(t *testing.T) {
	vm := &iavl.VersionMetadata{
		Version:   14,
		Committed: time.Now().UTC().Unix(),
		Updated:   time.Now().UTC().Unix(),
		RootHash:  []byte{0x04, 0x05, 0x00, 0xff, 0x04, 0x05, 0x00, 0xff},
		Snapshot:  true,
	}

	bz, err := vm.Marshal()
	require.NoError(t, err)
	require.NotNil(t, bz)

	vm2, err := iavl.UnmarshalVersionMetadata(bz)
	require.NoError(t, err)
	require.Equal(t, vm.String(), vm2.String())
}
