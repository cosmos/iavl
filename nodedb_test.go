package iavl

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	dbm "github.com/tendermint/tm-db"
)

func BenchmarkNodeKey(b *testing.B) {
	ndb := &nodeDB{}
	hashes := makeHashes(b, 2432325)
	for i := 0; i < b.N; i++ {
		ndb.nodeKey(hashes[i])
	}
}

func BenchmarkOrphanKey(b *testing.B) {
	ndb := &nodeDB{}
	hashes := makeHashes(b, 2432325)
	for i := 0; i < b.N; i++ {
		ndb.orphanKey(1234, 1239, hashes[i])
	}
}

func makeHashes(b *testing.B, seed int64) [][]byte {
	b.StopTimer()
	rnd := rand.NewSource(seed)
	hashes := make([][]byte, b.N)
	hashBytes := 8 * ((hashSize + 7) / 8)
	for i := 0; i < b.N; i++ {
		hashes[i] = make([]byte, hashBytes)
		for b := 0; b < hashBytes; b += 8 {
			binary.BigEndian.PutUint64(hashes[i][b:b+8], uint64(rnd.Int63()))
		}
		hashes[i] = hashes[i][:hashSize]
	}
	b.StartTimer()
	return hashes
}

func TestNodeDBVersionMetadata(t *testing.T) {
	memDB := dbm.NewMemDB()
	snapDB := dbm.NewMemDB()
	ndb := newNodeDB(snapDB, memDB, 0, nil)

	// Ensure metadata returns successfully when it was never saved to begin with
	// i.e. for backwards compatibility.
	vm, err := ndb.GetVersionMetadata(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), vm.Version)
	require.True(t, vm.Snapshot)

	x, ok := ndb.vmCache.Get(string(VersionMetadataKey(1)))
	require.False(t, ok)
	require.Nil(t, x)

	for i := 2; i < 50000; i++ {
		vm = &VersionMetadata{
			Version:  int64(i),
			Snapshot: i%2 == 0,
			RootHash: []byte(fmt.Sprintf("%d", i)),
		}
		require.NoError(t, ndb.SetVersionMetadata(vm))

		x, ok := ndb.vmCache.Get(string(VersionMetadataKey(vm.Version)))
		require.True(t, ok)
		require.NotNil(t, x)

		vm2, err := ndb.GetVersionMetadata(vm.Version)
		require.NoError(t, err)
		require.Equal(t, vm, vm2)

		require.NoError(t, ndb.DeleteVersionMetadata(vm.Version))

		x, ok = ndb.vmCache.Get(string(VersionMetadataKey(vm.Version)))
		require.False(t, ok)
		require.Nil(t, x)
	}
}
