package iavl

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	db "github.com/tendermint/tm-db"

	"github.com/cosmos/iavl/mock"
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

func TestNewNoDbStorage_StorageVersionInDb_Success(t *testing.T) {
	const expectedVersion = fastStorageVersionValue
	
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(expectedVersion), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	
	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, ndb.storageVersion)
}

func TestNewNoDbStorage_ErrorInConstructor_DefaultSet(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue
	
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, errors.New("some db error")).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	
	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, string(ndb.getStorageVersion()))
}

func TestNewNoDbStorage_DoesNotExist_DefaultSet(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue
	
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	
	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, expectedVersion, string(ndb.getStorageVersion()))
}

func TestSetStorageVersion_Success(t *testing.T) {
	const expectedVersion = fastStorageVersionValue
	
	db := db.NewMemDB()
	
	ndb := newNodeDB(db, 0, nil)
	require.Equal(t, defaultStorageVersionValue, string(ndb.getStorageVersion()))

	err := ndb.setStorageVersion(expectedVersion)
	require.NoError(t, err)
	require.Equal(t, expectedVersion, string(ndb.getStorageVersion()))
}

func TestSetStorageVersion_Failure_OldKept(t *testing.T) {
	ctrl := gomock.NewController(t)
	
	dbMock := mock.NewMockDB(ctrl)
	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(defaultStorageVersionValue), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)
	dbMock.EXPECT().Set(gomock.Any(), gomock.Any()).Return(errors.New("some db error")).Times(1)

	ndb := newNodeDB(dbMock, 0, nil)
	require.Equal(t, defaultStorageVersionValue, string(ndb.getStorageVersion()))

	ndb.setStorageVersion(fastStorageVersionValue)
	require.Equal(t, defaultStorageVersionValue, string(ndb.getStorageVersion()))
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
