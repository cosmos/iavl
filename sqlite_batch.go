package iavl

import (
	"fmt"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
)

type sqliteBatch struct {
	queue             *writeQueue
	version           int64
	storeLatestLeaves bool
	sql               *SqliteDb
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
	if err = b.sql.leafWrite.Begin(); err != nil {
		return err
	}
	b.leafInsert, err = b.sql.leafWrite.Prepare("INSERT OR REPLACE INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.deleteInsert, err = b.sql.leafWrite.Prepare("INSERT OR REPLACE INTO leaf_delete (version, sequence, key) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.latestInsert, err = b.sql.leafWrite.Prepare("INSERT OR REPLACE INTO latest (key, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	b.latestDelete, err = b.sql.leafWrite.Prepare("DELETE FROM latest WHERE key = ?")
	if err != nil {
		return err
	}
	b.leafOrphan, err = b.sql.leafWrite.Prepare("INSERT INTO leaf_orphan (version, sequence, at) VALUES (?, ?, ?)")
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
	if err := b.sql.leafWrite.Commit(); err != nil {
		return err
	}
	if err := b.leafInsert.Close(); err != nil {
		return err
	}
	if err := b.deleteInsert.Close(); err != nil {
		return err
	}
	if err := b.latestInsert.Close(); err != nil {
		return err
	}
	if err := b.latestDelete.Close(); err != nil {
		return err
	}
	if err := b.leafOrphan.Close(); err != nil {
		return err
	}

	return nil
}

func (b *sqliteBatch) execBranchOrphan(nodeKey NodeKey) error {
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.version)
}

func (b *sqliteBatch) newTreeBatch(shardID int64) (err error) {
	if err = b.sql.treeWrite.Begin(); err != nil {
		return err
	}
	b.treeInsert, err = b.sql.treeWrite.Prepare(fmt.Sprintf(
		"INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", shardID))
	if err != nil {
		return err
	}
	b.treeOrphan, err = b.sql.treeWrite.Prepare("INSERT INTO orphan (version, sequence, at) VALUES (?, ?, ?)")
	b.treeSince = time.Now()
	return err
}

func (b *sqliteBatch) treeBatchCommit() error {
	if err := b.sql.treeWrite.Commit(); err != nil {
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

func (b *sqliteBatch) treeMaybeCommit(shardID int64) (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommit(); err != nil {
			return err
		}
		if err = b.newTreeBatch(shardID); err != nil {
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
		if err = b.leafInsert.Exec(leaf.nodeKey.Version(), int(leaf.nodeKey.Sequence()), bz); err != nil {
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

	err = b.sql.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence)")
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

		shardID, err := b.sql.nextShard(b.version)
		if err != nil {
			return 0, err
		}
		b.logger.Debug().Msgf("checkpoint db=tree version=%d shard=%d orphans=%s",
			b.version, shardID, humanize.Comma(int64(len(b.queue.branchOrphans))))

		if err = b.newTreeBatch(shardID); err != nil {
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
			if err = b.treeMaybeCommit(shardID); err != nil {
				return 0, err
			}
		}

		for _, orphan := range b.queue.branchOrphans {
			b.treeCount++
			err = b.execBranchOrphan(orphan)
			if err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(shardID); err != nil {
				return 0, err
			}
		}

		if err = b.treeBatchCommit(); err != nil {
			return 0, err
		}
		err = b.sql.treeWrite.Exec(fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", shardID, shardID))
		if err != nil {
			return 0, err
		}
	}

	return b.treeCount, nil
}
