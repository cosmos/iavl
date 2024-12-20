package iavl

import (
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
)

type sqliteBatch struct {
	queue             *writeQueue
	version           int64
	storeLatestLeaves bool
	conn              *sqlite3.Conn
	size              int64
	logger            zerolog.Logger

	treeCount int64
	treeSince time.Time
	leafCount int64
	leafSince time.Time

	leafInsert   *sqlite3.Stmt
	deleteInsert *sqlite3.Stmt
	latestInsert *sqlite3.Stmt
	latestDelete *sqlite3.Stmt
	treeInsert   *sqlite3.Stmt
	leafOrphan   *sqlite3.Stmt
	treeOrphan   *sqlite3.Stmt
}

func (b *sqliteBatch) newChangeLogBatch() (err error) {
	if err = b.conn.Begin(); err != nil {
		return err
	}
	b.leafInsert, err = b.conn.Prepare("INSERT OR REPLACE INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.deleteInsert, err = b.conn.Prepare("INSERT OR REPLACE INTO leaf_delete (version, sequence, key) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	if b.storeLatestLeaves {
		b.latestInsert, err = b.conn.Prepare("INSERT OR REPLACE INTO latest (key, value) VALUES (?, ?)")
		if err != nil {
			return err
		}
		b.latestDelete, err = b.conn.Prepare("DELETE FROM latest WHERE key = ?")
		if err != nil {
			return err
		}
	}
	b.leafOrphan, err = b.conn.Prepare("INSERT INTO leaf_orphan (version, sequence, at) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.leafSince = time.Now()
	return nil
}

func (b *sqliteBatch) changelogMaybeCommit() (err error) {
	if b.leafCount%b.size == 0 {
		if err = b.changelogBatchCommit(); err != nil {
			return err
		}
		if err = b.newChangeLogBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) changelogBatchCommit() error {
	if err := b.conn.Commit(); err != nil {
		return err
	}
	if err := b.leafInsert.Close(); err != nil {
		return err
	}
	if err := b.deleteInsert.Close(); err != nil {
		return err
	}
	if b.storeLatestLeaves {
		if err := b.latestInsert.Close(); err != nil {
			return err
		}
		if err := b.latestDelete.Close(); err != nil {
			return err
		}
	}
	if err := b.leafOrphan.Close(); err != nil {
		return err
	}

	return nil
}

func (b *sqliteBatch) execBranchOrphan(nodeKey NodeKey) error {
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.version)
}

func (b *sqliteBatch) newTreeBatch() (err error) {
	if err = b.conn.Begin(); err != nil {
		return err
	}
	b.treeInsert, err = b.conn.Prepare("INSERT INTO tree (version, sequence, bytes) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.treeOrphan, err = b.conn.Prepare("INSERT INTO orphan (version, sequence, at) VALUES (?, ?, ?)")
	b.treeSince = time.Now()
	return err
}

func (b *sqliteBatch) treeBatchCommit() error {
	if err := b.conn.Commit(); err != nil {
		return err
	}
	if err := b.treeInsert.Close(); err != nil {
		return err
	}
	if err := b.treeOrphan.Close(); err != nil {
		return err
	}

	if b.treeCount >= b.size {
		batchSize := b.treeCount % b.size
		if batchSize == 0 {
			batchSize = b.size
		}
		b.logger.Debug().Msgf("db=tree count=%s dur=%s batch=%d rate=%s",
			humanize.Comma(b.treeCount),
			time.Since(b.treeSince).Round(time.Millisecond),
			batchSize,
			humanize.Comma(int64(float64(batchSize)/time.Since(b.treeSince).Seconds())))
	}
	return nil
}

func (b *sqliteBatch) treeMaybeCommit() (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommit(); err != nil {
			return err
		}
		if err = b.newTreeBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) saveLeaves() (int64, error) {
	var byteCount int64

	err := b.newChangeLogBatch()
	if err != nil {
		return 0, err
	}

	var (
		bz  []byte
		val []byte
	)
	for _, leaf := range b.queue.leaves {
		b.leafCount++
		if b.storeLatestLeaves {
			val = leaf.value
			leaf.value = nil
		}
		bz, err = leaf.Bytes()
		if err != nil {
			return 0, err
		}
		byteCount += int64(len(bz))
		if err = b.leafInsert.Exec(leaf.Version(), int(leaf.nodeKey.Sequence()), bz); err != nil {
			return 0, err
		}
		if b.storeLatestLeaves {
			if err = b.latestInsert.Exec(leaf.key, val); err != nil {
				return 0, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, leafDelete := range b.queue.deletes {
		b.leafCount++
		err = b.deleteInsert.Exec(leafDelete.deleteKey.Version(), int(leafDelete.deleteKey.Sequence()), leafDelete.leafKey)
		if err != nil {
			return 0, err
		}
		if b.storeLatestLeaves {
			if err = b.latestDelete.Exec(leafDelete.leafKey); err != nil {
				return 0, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, orphan := range b.queue.leafOrphans {
		b.leafCount++
		err = b.leafOrphan.Exec(orphan.Version(), int(orphan.Sequence()), b.version)
		if err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	if err = b.changelogBatchCommit(); err != nil {
		return 0, err
	}

	err = b.conn.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence)")
	if err != nil {
		return byteCount, err
	}

	return b.leafCount, nil
}

func (b *sqliteBatch) isCheckpoint() bool {
	return len(b.queue.branches) > 0
}

func (b *sqliteBatch) saveBranches() (n int64, err error) {
	if b.isCheckpoint() {
		b.treeCount = 0

		b.logger.Debug().Msgf("checkpoint db=tree version=%d branches=%s orphans=%s",
			b.version,
			humanize.Comma(int64(len(b.queue.branches))),
			humanize.Comma(int64(len(b.queue.branchOrphans))))

		if err = b.newTreeBatch(); err != nil {
			return 0, err
		}

		for _, node := range b.queue.branches {
			b.treeCount++
			bz, err := node.Bytes()
			if err != nil {
				return 0, err
			}
			if err = b.treeInsert.Exec(node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz); err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(); err != nil {
				return 0, err
			}
		}

		for _, orphan := range b.queue.branchOrphans {
			b.treeCount++
			err = b.execBranchOrphan(orphan)
			if err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(); err != nil {
				return 0, err
			}
		}

		if err = b.treeBatchCommit(); err != nil {
			return 0, err
		}
		err = b.conn.Exec("CREATE INDEX IF NOT EXISTS tree_idx ON tree (version, sequence);")
		if err != nil {
			return 0, err
		}
	}

	return b.treeCount, nil
}
