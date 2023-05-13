package iavl

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	log "cosmossdk.io/log"
	db "github.com/cosmos/cosmos-db"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cosmos/iavl/mock"
)

func BenchmarkNodeKey(b *testing.B) {
	ndb := &nodeDB{}
	for i := 0; i < b.N; i++ {
		ndb.nodeKey(&NodeKey{
			version: int64(i),
			nonce:   int32(i),
		})
	}
}

func BenchmarkTreeString(b *testing.B) {
	tree := makeAndPopulateMutableTree(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink, _ = tree.String()
		require.NotNil(b, sink)
	}

	if sink == nil {
		b.Fatal("Benchmark did not run")
	}
	sink = (interface{})(nil)
}

func TestNewNoDbStorage_StorageVersionInDb_Success(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue

	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(expectedVersion), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	ndb := newNodeDB(dbMock, 0, nil, log.NewNopLogger())
	require.Equal(t, expectedVersion, ndb.storageVersion)
}

func TestNewNoDbStorage_ErrorInConstructor_DefaultSet(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue

	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, errors.New("some db error")).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	ndb := newNodeDB(dbMock, 0, nil, log.NewNopLogger())
	require.Equal(t, expectedVersion, ndb.getStorageVersion())
}

func TestNewNoDbStorage_DoesNotExist_DefaultSet(t *testing.T) {
	const expectedVersion = defaultStorageVersionValue

	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)

	dbMock.EXPECT().Get(gomock.Any()).Return(nil, nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(nil).Times(1)

	ndb := newNodeDB(dbMock, 0, nil, log.NewNopLogger())
	require.Equal(t, expectedVersion, ndb.getStorageVersion())
}

func TestSetStorageVersion_Success(t *testing.T) {
	const expectedVersion = fastStorageVersionValue

	db := db.NewMemDB()

	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	require.Equal(t, defaultStorageVersionValue, ndb.getStorageVersion())

	err := ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)

	latestVersion, err := ndb.getLatestVersion()
	require.NoError(t, err)
	require.Equal(t, expectedVersion+fastStorageVersionDelimiter+strconv.Itoa(int(latestVersion)), ndb.getStorageVersion())
	require.NoError(t, ndb.batch.Write())
}

func TestSetStorageVersion_DBFailure_OldKept(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)
	batchMock := mock.NewMockBatch(ctrl)
	rIterMock := mock.NewMockIterator(ctrl)

	expectedErrorMsg := "some db error"

	expectedFastCacheVersion := 2

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(defaultStorageVersionValue), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(batchMock).Times(1)

	// rIterMock is used to get the latest version from disk. We are mocking that rIterMock returns latestTreeVersion from disk
	rIterMock.EXPECT().Valid().Return(true).Times(1)
	rIterMock.EXPECT().Key().Return(nodeKeyFormat.Key(expectedFastCacheVersion)).Times(1)
	rIterMock.EXPECT().Close().Return(nil).Times(1)

	dbMock.EXPECT().ReverseIterator(gomock.Any(), gomock.Any()).Return(rIterMock, nil).Times(1)
	batchMock.EXPECT().Set(metadataKeyFormat.Key([]byte(storageVersionKey)), []byte(fastStorageVersionValue+fastStorageVersionDelimiter+strconv.Itoa(expectedFastCacheVersion))).Return(errors.New(expectedErrorMsg)).Times(1)

	ndb := newNodeDB(dbMock, 0, nil, log.NewNopLogger())
	require.Equal(t, defaultStorageVersionValue, ndb.getStorageVersion())

	err := ndb.setFastStorageVersionToBatch()
	require.Error(t, err)
	require.Equal(t, expectedErrorMsg, err.Error())
	require.Equal(t, defaultStorageVersionValue, ndb.getStorageVersion())
}

func TestSetStorageVersion_InvalidVersionFailure_OldKept(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := mock.NewMockDB(ctrl)
	batchMock := mock.NewMockBatch(ctrl)

	expectedErrorMsg := errInvalidFastStorageVersion

	invalidStorageVersion := fastStorageVersionValue + fastStorageVersionDelimiter + "1" + fastStorageVersionDelimiter + "2"

	dbMock.EXPECT().Get(gomock.Any()).Return([]byte(invalidStorageVersion), nil).Times(1)
	dbMock.EXPECT().NewBatch().Return(batchMock).Times(1)

	ndb := newNodeDB(dbMock, 0, nil, log.NewNopLogger())
	require.Equal(t, invalidStorageVersion, ndb.getStorageVersion())

	err := ndb.setFastStorageVersionToBatch()
	require.Error(t, err)
	require.Equal(t, expectedErrorMsg, err)
	require.Equal(t, invalidStorageVersion, ndb.getStorageVersion())
}

func TestSetStorageVersion_FastVersionFirst_VersionAppended(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.storageVersion = fastStorageVersionValue
	ndb.latestVersion = 100

	err := ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	require.Equal(t, fastStorageVersionValue+fastStorageVersionDelimiter+strconv.Itoa(int(ndb.latestVersion)), ndb.storageVersion)
}

func TestSetStorageVersion_FastVersionSecond_VersionAppended(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.latestVersion = 100

	storageVersionBytes := []byte(fastStorageVersionValue)
	storageVersionBytes[len(fastStorageVersionValue)-1]++ // increment last byte
	ndb.storageVersion = string(storageVersionBytes)

	err := ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	require.Equal(t, string(storageVersionBytes)+fastStorageVersionDelimiter+strconv.Itoa(int(ndb.latestVersion)), ndb.storageVersion)
}

func TestSetStorageVersion_SameVersionTwice(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.latestVersion = 100

	storageVersionBytes := []byte(fastStorageVersionValue)
	storageVersionBytes[len(fastStorageVersionValue)-1]++ // increment last byte
	ndb.storageVersion = string(storageVersionBytes)

	err := ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	newStorageVersion := string(storageVersionBytes) + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion))
	require.Equal(t, newStorageVersion, ndb.storageVersion)

	err = ndb.setFastStorageVersionToBatch()
	require.NoError(t, err)
	require.Equal(t, newStorageVersion, ndb.storageVersion)
}

// Test case where version is incorrect and has some extra garbage at the end
func TestShouldForceFastStorageUpdate_DefaultVersion_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.storageVersion = defaultStorageVersionValue
	ndb.latestVersion = 100

	shouldForce, err := ndb.shouldForceFastStorageUpgrade()
	require.False(t, shouldForce)
	require.NoError(t, err)
}

func TestShouldForceFastStorageUpdate_FastVersion_Greater_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion+1))

	shouldForce, err := ndb.shouldForceFastStorageUpgrade()
	require.True(t, shouldForce)
	require.NoError(t, err)
}

func TestShouldForceFastStorageUpdate_FastVersion_Smaller_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion-1))

	shouldForce, err := ndb.shouldForceFastStorageUpgrade()
	require.True(t, shouldForce)
	require.NoError(t, err)
}

func TestShouldForceFastStorageUpdate_FastVersion_Match_False(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion))

	shouldForce, err := ndb.shouldForceFastStorageUpgrade()
	require.False(t, shouldForce)
	require.NoError(t, err)
}

func TestIsFastStorageEnabled_True(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.latestVersion = 100
	ndb.storageVersion = fastStorageVersionValue + fastStorageVersionDelimiter + strconv.Itoa(int(ndb.latestVersion))

	require.True(t, ndb.hasUpgradedToFastStorage())
}

func TestIsFastStorageEnabled_False(t *testing.T) {
	db := db.NewMemDB()
	ndb := newNodeDB(db, 0, nil, log.NewNopLogger())
	ndb.latestVersion = 100
	ndb.storageVersion = defaultStorageVersionValue

	shouldForce, err := ndb.shouldForceFastStorageUpgrade()
	require.False(t, shouldForce)
	require.NoError(t, err)
}

func TestTraverseNodes(t *testing.T) {
	tree, _ := getTestTree(0)
	// version 1
	for i := 0; i < 20; i++ {
		_, err := tree.Set([]byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	_, _, err := tree.SaveVersion()
	require.NoError(t, err)
	// version 2, no commit
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	// version 3
	for i := 20; i < 30; i++ {
		_, err := tree.Set([]byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	count := 0
	err = tree.ndb.traverseNodes(func(node *Node) error {
		actualNode, err := tree.ndb.GetNode(node.nodeKey)
		if err != nil {
			return err
		}
		if actualNode.String() != node.String() {
			return fmt.Errorf("found unexpected node")
		}
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 64, count)
}

func assertOrphansAndBranches(t *testing.T, ndb *nodeDB, version int64, branches int, orphanKeys [][]byte) {
	var branchCount, orphanIndex int
	err := ndb.traverseOrphans(version, func(node *Node) error {
		if node.isLeaf() {
			require.Equal(t, orphanKeys[orphanIndex], node.key)
			orphanIndex++
		} else {
			branchCount++
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, branches, branchCount)
}

func TestNodeDB_traverseOrphans(t *testing.T) {
	tree, _ := getTestTree(0)
	var up bool
	var err error

	// version 1
	for i := 0; i < 20; i++ {
		up, err = tree.Set([]byte{byte(i)}, []byte{byte(i)})
		require.False(t, up)
		require.NoError(t, err)
	}
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	// note: assertions were constructed by hand after inspecting the output of the graphviz below.
	// WriteDOTGraphToFile("/tmp/tree_one.dot", tree.ImmutableTree)

	// version 2
	up, err = tree.Set([]byte{byte(19)}, []byte{byte(0)})
	require.True(t, up)
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	// WriteDOTGraphToFile("/tmp/tree_two.dot", tree.ImmutableTree)

	assertOrphansAndBranches(t, tree.ndb, 1, 5, [][]byte{{byte(19)}})

	// version 3
	k, up, err := tree.Remove([]byte{byte(0)})
	require.Equal(t, []byte{byte(0)}, k)
	require.True(t, up)
	require.NoError(t, err)

	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	// WriteDOTGraphToFile("/tmp/tree_three.dot", tree.ImmutableTree)

	assertOrphansAndBranches(t, tree.ndb, 2, 4, [][]byte{{byte(0)}})

	// version 4
	k, up, err = tree.Remove([]byte{byte(1)})
	require.Equal(t, []byte{byte(1)}, k)
	require.True(t, up)
	require.NoError(t, err)
	k, up, err = tree.Remove([]byte{byte(19)})
	require.Equal(t, []byte{byte(0)}, k)
	require.True(t, up)
	require.NoError(t, err)

	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	// WriteDOTGraphToFile("/tmp/tree_four.dot", tree.ImmutableTree)

	assertOrphansAndBranches(t, tree.ndb, 3, 7, [][]byte{{byte(1)}, {byte(19)}})

	// version 5
	k, up, err = tree.Remove([]byte{byte(10)})
	require.Equal(t, []byte{byte(10)}, k)
	require.True(t, up)
	require.NoError(t, err)
	k, up, err = tree.Remove([]byte{byte(9)})
	require.Equal(t, []byte{byte(9)}, k)
	require.True(t, up)
	require.NoError(t, err)
	up, err = tree.Set([]byte{byte(12)}, []byte{byte(0)})
	require.True(t, up)
	require.NoError(t, err)

	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	// WriteDOTGraphToFile("/tmp/tree_five.dot", tree.ImmutableTree)

	assertOrphansAndBranches(t, tree.ndb, 4, 8, [][]byte{{byte(9)}, {byte(10)}, {byte(12)}})
}

func makeAndPopulateMutableTree(tb testing.TB) *MutableTree {
	memDB := db.NewMemDB()
	tree, err := NewMutableTreeWithOpts(memDB, 0, &Options{InitialVersion: 9}, false, log.NewNopLogger())
	require.NoError(tb, err)

	for i := 0; i < 1e4; i++ {
		buf := make([]byte, 0, (i/255)+1)
		for j := 0; 1<<j <= i; j++ {
			buf = append(buf, byte((i>>j)&0xff))
		}
		tree.Set(buf, buf) //nolint:errcheck
	}
	_, _, err = tree.SaveVersion()
	require.Nil(tb, err, "Expected .SaveVersion to succeed")
	return tree
}
