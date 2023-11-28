package iavl

import (
	"fmt"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
)

type sqliteBatch struct {
	sql    *SqliteDb
	size   int
	logger zerolog.Logger

	count int
	since time.Time

	leafInsert   *sqlite3.Stmt
	deleteInsert *sqlite3.Stmt
	latestInsert *sqlite3.Stmt
	latestDelete *sqlite3.Stmt
	treeInsert   *sqlite3.Stmt
}

func (sql *SqliteDb) newSqliteBatch() *sqliteBatch {
	return &sqliteBatch{
		sql:  sql,
		size: 200_000,
		logger: log.With().
			Str("module", "sqlite-batch").
			Str("path", sql.opts.Path).Logger(),
	}
}

func (b *sqliteBatch) newChangeLogBatch() (err error) {
	if err = b.sql.leafWrite.Begin(); err != nil {
		return err
	}
	b.leafInsert, err = b.sql.leafWrite.Prepare("INSERT INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.deleteInsert, err = b.sql.leafWrite.Prepare("INSERT INTO leaf_delete (version, sequence, key) VALUES (?, ?, ?)")
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
	b.since = time.Now()
	return nil
}

func (b *sqliteBatch) changelogMaybeCommit() (err error) {
	if b.count%b.size == 0 {
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

	if b.count > b.size {
		b.logger.Debug().Msgf("db=changelog count=%s dur=%s rate=%s",
			humanize.Comma(int64(b.count)),
			time.Since(b.since).Round(time.Millisecond),
			humanize.Comma(int64(float64(b.size)/time.Since(b.since).Seconds())))
	}

	return nil
}

func (b *sqliteBatch) newTreeBatch() (err error) {
	if err = b.sql.treeWrite.Begin(); err != nil {
		return err
	}
	b.treeInsert, err = b.sql.treeWrite.Prepare(fmt.Sprintf(
		"INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", b.sql.shardId))
	return err
}

func (b *sqliteBatch) treeBatchCommit() error {
	if err := b.sql.treeWrite.Commit(); err != nil {
		return err
	}
	if err := b.treeInsert.Close(); err != nil {
		return err
	}
	if b.count > b.size {
		b.logger.Debug().Msgf("db=tree count=%s dur=%s rate=%s",
			humanize.Comma(int64(b.count)),
			time.Since(b.since).Round(time.Millisecond),
			humanize.Comma(int64(float64(b.size)/time.Since(b.since).Seconds())))
	}
	return nil
}

func (b *sqliteBatch) treeMaybeCommit() (err error) {
	if b.count%b.size == 0 {
		if err = b.treeBatchCommit(); err != nil {
			return err
		}
		if err = b.newTreeBatch(); err != nil {
			return err
		}
		b.since = time.Now()
	}
	return nil
}

func (b *sqliteBatch) saveTree(tree *Tree) (n int64, versions []int64, err error) {
	var byteCount int64
	versionMap := make(map[int64]bool)

	err = b.newChangeLogBatch()
	if err != nil {
		return 0, versions, err
	}

	var (
		bz  []byte
		val []byte
	)
	for _, leaf := range tree.leaves {
		b.count++
		if tree.storeLatestLeaves {
			val = leaf.value
			leaf.value = nil
		}
		bz, err = leaf.Bytes()
		if err != nil {
			return 0, nil, err
		}
		byteCount += int64(len(bz))
		if err = b.leafInsert.Exec(leaf.nodeKey.Version(), int(leaf.nodeKey.Sequence()), bz); err != nil {
			return 0, nil, err
		}
		if tree.storeLatestLeaves {
			if err = b.latestInsert.Exec(leaf.key, val); err != nil {
				return 0, nil, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, versions, err
		}
		if tree.heightFilter > 0 {
			b.sql.pool.Put(leaf)
		}
	}

	for _, leafDelete := range tree.deletes {
		b.count++
		err = b.deleteInsert.Exec(leafDelete.deleteKey.Version(), int(leafDelete.deleteKey.Sequence()), leafDelete.leafKey)
		if err != nil {
			return 0, nil, err
		}
		if tree.storeLatestLeaves {
			if err = b.latestDelete.Exec(leafDelete.leafKey); err != nil {
				return 0, nil, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, versions, err
		}
	}

	if err = b.changelogBatchCommit(); err != nil {
		return 0, versions, err
	}

	if len(tree.branches) > 0 {
		if err = b.newTreeBatch(); err != nil {
			return 0, nil, err
		}

		for _, node := range tree.branches {
			b.count++
			versionMap[node.nodeKey.Version()] = true
			bz, err = node.Bytes()
			if err != nil {
				return 0, nil, err
			}
			if err = b.treeInsert.Exec(node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz); err != nil {
				return 0, nil, err
			}
			if err = b.treeMaybeCommit(); err != nil {
				return 0, versions, err
			}
		}

		if err = b.treeBatchCommit(); err != nil {
			return 0, versions, err
		}
		err = b.sql.treeWrite.Exec(fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);",
			b.sql.shardId, b.sql.shardId))
		if err != nil {
			return 0, versions, err
		}
	}

	//err = b.sql.leafWrite.Exec("PRAGMA wal_checkpoint(RESTART);")
	//if err != nil {
	//	return 0, versions, err
	//}

	for version := range versionMap {
		versions = append(versions, version)
	}
	return byteCount, versions, nil
}
