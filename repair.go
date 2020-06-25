package iavl

import (
	"github.com/pkg/errors"
	dbm "github.com/tendermint/tm-db"
)

// Repair013Orphans repairs incorrect orphan entries written by IAVL 0.13 pruning. To use it, close
// a database using IAVL 0.13, make a backup copy, and then run this function before opening the
// database with IAVL 0.14 or later. It returns the number of faulty orphan entries removed.
//
// Note that this cannot be used directly on Cosmos SDK databases, since they store multiple IAVL
// trees in the same underlying database via a prefix scheme.
//
// The pruning functionality enabled with Options.KeepEvery > 1 would write orphans entries to disk
// for versions that should only have been saved in memory, and these orphan entries were clamped
// to the last version persisted to disk instead of the version that generated them. If the
// database was reopened at the last persisted version and this version was later deleted, the
// orphaned nodes could be deleted prematurely or incorrectly, causing data loss and database
// corruption.
//
// This function removes these incorrect orphan entries by deleting all orphan entries that have a
// to-version equal to or greater than the latest persisted version. Correct orphans will never
// have this, since they must have been deleted in the next (non-existent) version for that to be
// the case.
func Repair013Orphans(db dbm.DB) (uint64, error) {
	ndb := newNodeDB(db, 0, &Options{Sync: true})
	version := ndb.getLatestVersion()
	if version == 0 {
		return 0, errors.New("no versions found")
	}

	batch := db.NewBatch()
	repaired := uint64(0)
	ndb.traverseOrphans(func(key, hash []byte) {
		var toVersion int64
		orphanKeyFormat.Scan(key, &toVersion)
		if toVersion >= version {
			repaired++
			batch.Delete(key)
		}
	})
	err := batch.WriteSync()
	if err != nil {
		return 0, err
	}

	return repaired, nil
}
