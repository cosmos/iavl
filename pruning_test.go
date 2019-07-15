package iavl

import (
	"encoding/binary"
	"fmt"
	"os"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/libs/db"
)

func getTestDBs() (db.DB, db.DB, func()) {
	d, err := db.NewGoLevelDB("test", ".")
	if err != nil {
		panic(err)
	}
	return d, db.NewMemDB(), func() {
		d.Close()
		os.RemoveAll("./test.db")
	}
}

func TestSave(t *testing.T) {
	db, mdb, close := getTestDBs()
	defer close()

	keepRecent := rand.Int63n(8) + 2 //keep at least 2 versions in memDB
	keepEvery := (rand.Int63n(3) + 1) * 100
	mt := NewMutableTreePruningOpts(db, mdb, 5, keepEvery, keepRecent)

	// create 1000 versions
	for i := 0; i < 1000; i++ {
		// set 5 keys per version
		for j := 0; j < 5; j++ {
			key := make([]byte, 8)
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(rand.Int63()))
			binary.BigEndian.PutUint64(val, uint64(rand.Int63()))
			mt.Set(key, val)
		}
		_, _, err := mt.SaveVersion()
		require.Nil(t, err, "SaveVersion failed")
	}

	versions := mt.AvailableVersions()
	// check that all available versions are expected.
	for _, v := range versions {
		ver := int64(v)
		// check that version is supposed to exist given pruning strategy
		require.True(t, ver%keepEvery == 0 || mt.Version()-ver <= keepRecent,
			fmt.Sprintf("Version: %d should not exist. KeepEvery: %d, KeepRecent: %d", v, keepEvery, keepRecent))

		// check that root exists in nodeDB
		lv, err := mt.LazyLoadVersion(ver)
		require.Equal(t, ver, lv, "Version returned by LazyLoadVersion is wrong")
		require.Nil(t, err, "Version should exist in nodeDB")
	}

	// check all expected versions are available.
	for j := keepEvery; j <= mt.Version(); j += keepEvery {
		require.True(t, mt.VersionExists(int64(j)), fmt.Sprintf("Expected snapshot version: %d to be available in nodeDB. KeepEvery: %d, KeepRecent: %d", j, keepEvery, keepRecent))
	}
	for k := mt.Version()-keepRecent+1; k <= mt.Version(); k++ {
		require.True(t, mt.VersionExists(int64(k)), fmt.Sprintf("Expected recent version: %d to be available in nodeDB. KeepEvery: %d, KeepRecent: %d", k, keepEvery, keepRecent))
	}

	// check that there only exists correct number of roots in nodeDB
	roots, err := mt.ndb.getRoots()
	require.Nil(t, err, "Error in getRoots")
	numRoots := 1000 / keepEvery + keepRecent
	// decrement if there is overlap between snapshot and recent versions
	if 1000 % keepEvery == 0 {
		numRoots--
	}
	require.Equal(t, numRoots, int64(len(roots)), "nodeDB does not contain expected number of roots")
}

func TestDeleteOrphans(t *testing.T) {
	db, mdb, close := getTestDBs()
	defer close()

	keepRecent := rand.Int63n(8) + 2 //keep at least 2 versions in memDB
	keepEvery := (rand.Int63n(3) + 1) * 100
	mt := NewMutableTreePruningOpts(db, mdb, 5, keepEvery, keepRecent)

	// create 1000 versions
	for i := 0; i < 1000; i++ {
		// set 5 keys per version
		for j := 0; j < 5; j++ {
			key := make([]byte, 8)
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(rand.Int63()))
			binary.BigEndian.PutUint64(val, uint64(rand.Int63()))
			mt.Set(key, val)
		}
		_, _, err := mt.SaveVersion()
		require.Nil(t, err, "SaveVersion failed")
	}

	snapfn := func(key, v []byte) {
		var fromVersion, toVersion int64

		// See comment on `orphanKeyFmt`. Note that here, `version` and
		// `toVersion` are always equal.
		orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

		// toVersion must be snapshotVersion
		require.True(t, toVersion%keepEvery == 0, fmt.Sprintf("Orphan in snapshotDB has unexpected toVersion: %d. Should never have been persisted", toVersion))
	}

	// check orphans in snapshotDB are expected
	traverseOrphansFromDB(mt.ndb.snapshotDB, snapfn)

	recentFn := func(key, v []byte) {
		var fromVersion, toVersion int64

		// See comment on `orphanKeyFmt`. Note that here, `version` and
		// `toVersion` are always equal.
		orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

		// toVersion must exist in recentDB
		require.True(t, toVersion > mt.Version() - keepRecent, fmt.Sprintf("Orphan in recentDB has unexpected fromVersion: %d. Should have been deleted", fromVersion))
	}

	// check orphans in recentDB are expected
	traverseOrphansFromDB(mt.ndb.recentDB, recentFn)

	// delete snapshot orphans except latest version
	for j := keepEvery; j < mt.Version(); j += keepEvery {
		err := mt.DeleteVersion(j)
		require.Nil(t, err, fmt.Sprintf("Could not delete version %d", j))
	}

	lastfn := func(key, v []byte) {
		var fromVersion, toVersion int64

		// See comment on `orphanKeyFmt`. Note that here, `version` and
		// `toVersion` are always equal.
		orphanKeyFormat.Scan(key, &toVersion, &fromVersion)

		// only orphans that should exist have toVersion = latestVersion
		require.Equal(t, mt.Version(), toVersion, fmt.Sprintf("Unexpected Orphan with toVersion: %d", toVersion))
	}
	traverseOrphansFromDB(mt.ndb.snapshotDB, lastfn)

	// delete all recent orphans escept latest version
	for k := mt.Version()-keepRecent+1; k < mt.Version(); k++ {
		err := mt.DeleteVersion(k)
		require.Nil(t, err, fmt.Sprintf("Could not delete version %d", k))
	}
	traverseOrphansFromDB(mt.ndb.recentDB, lastfn)
}
