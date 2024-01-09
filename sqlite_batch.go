package iavl

import (
	"fmt"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
)

type sqliteBatch struct {
	tree   *Tree
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

	leafOrphan  *sqlite3.Stmt
	treeOrphans map[int64]*sqlite3.Stmt
	treeOrphan  *sqlite3.Stmt
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
	if err := b.leafOrphan.Close(); err != nil {
		return err
	}

	if b.count >= b.size {
		b.logger.Debug().Msgf("db=changelog count=%s dur=%s rate=%s",
			humanize.Comma(int64(b.count)),
			time.Since(b.since).Round(time.Millisecond),
			humanize.Comma(int64(float64(b.size)/time.Since(b.since).Seconds())))
	}

	return nil
}

func (b *sqliteBatch) execBranchOrphan(nodeKey NodeKey) error {
	//version := nodeKey.Version()
	//checkpointVersion := b.sql.shards.Find(version)
	//if checkpointVersion == -1 {
	//	return fmt.Errorf("version %d not found", version)
	//}
	//stmt, ok := b.treeOrphans[version]
	//if !ok {
	//	var err error
	//	sqlStmt := fmt.Sprintf("UPDATE tree_%d SET orphaned = true WHERE version = ? AND sequence = ?",
	//		checkpointVersion)
	//	stmt, err = b.sql.treeWrite.Prepare(sqlStmt)
	//	if err != nil {
	//		return err
	//	}
	//	b.treeOrphans[version] = stmt
	//}
	//
	//return stmt.Exec(version, int(nodeKey.Sequence()))
	return b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.tree.version)
}

func (b *sqliteBatch) newTreeBatch(checkpointVersion int64) (err error) {
	if err = b.sql.treeWrite.Begin(); err != nil {
		return err
	}
	b.treeInsert, err = b.sql.treeWrite.Prepare(fmt.Sprintf(
		"INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", checkpointVersion))
	b.treeOrphans = make(map[int64]*sqlite3.Stmt)
	b.treeOrphan, err = b.sql.treeWrite.Prepare("INSERT INTO orphan (version, sequence, at) VALUES (?, ?, ?)")
	b.since = time.Now()
	return err
}

func (b *sqliteBatch) treeBatchCommit() error {
	if err := b.sql.treeWrite.Commit(); err != nil {
		return err
	}
	if err := b.treeInsert.Close(); err != nil {
		return err
	}
	for _, stmt := range b.treeOrphans {
		if err := stmt.Close(); err != nil {
			return err
		}
	}
	if err := b.treeOrphan.Close(); err != nil {
		return err
	}

	if b.count >= b.size {
		batchSize := b.count % b.size
		if batchSize == 0 {
			batchSize = b.size
		}
		b.logger.Debug().Msgf("db=tree count=%s dur=%s batch=%d rate=%s",
			humanize.Comma(int64(b.count)),
			time.Since(b.since).Round(time.Millisecond),
			batchSize,
			humanize.Comma(int64(float64(batchSize)/time.Since(b.since).Seconds())))
	}
	return nil
}

func (b *sqliteBatch) treeMaybeCommit(checkpointVersion int64) (err error) {
	if b.count%b.size == 0 {
		if err = b.treeBatchCommit(); err != nil {
			return err
		}
		if err = b.newTreeBatch(checkpointVersion); err != nil {
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
		bz   []byte
		val  []byte
		tree = b.tree
	)
	for i, leaf := range tree.leaves {
		b.count++
		if tree.storeLatestLeaves {
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
		if tree.storeLatestLeaves {
			if err = b.latestInsert.Exec(leaf.key, val); err != nil {
				return 0, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
		if tree.heightFilter > 0 {
			if i != 0 {
				// evict leaf
				b.sql.pool.Put(leaf)
			} else if leaf.nodeKey != tree.root.nodeKey {
				// never evict the root if it's a leaf
				b.sql.pool.Put(leaf)
			}
		}
	}

	for _, leafDelete := range tree.deletes {
		b.count++
		err = b.deleteInsert.Exec(leafDelete.deleteKey.Version(), int(leafDelete.deleteKey.Sequence()), leafDelete.leafKey)
		if err != nil {
			return 0, err
		}
		if tree.storeLatestLeaves {
			if err = b.latestDelete.Exec(leafDelete.leafKey); err != nil {
				return 0, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, orphan := range tree.leafOrphans {
		b.count++
		err = b.leafOrphan.Exec(orphan.Version(), int(orphan.Sequence()), b.tree.version)
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

	err = tree.sql.leafWrite.Exec("CREATE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence)")
	if err != nil {
		return byteCount, err
	}

	return byteCount, nil
}

func (b *sqliteBatch) isCheckpoint() bool {
	return len(b.tree.branches) > 0
}

func (b *sqliteBatch) saveBranches() (n int64, err error) {
	if b.isCheckpoint() {
		tree := b.tree
		b.count = 0

		log.Info().Msgf("checkpointing version=%d path=%s", tree.version, tree.sql.opts.Path)
		if err := tree.sql.NextShard(tree.version); err != nil {
			return 0, err
		}

		if err = b.newTreeBatch(tree.version); err != nil {
			return 0, err
		}

		for _, node := range tree.branches {
			b.count++
			bz, err := node.Bytes()
			if err != nil {
				return 0, err
			}
			if err = b.treeInsert.Exec(node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz); err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(tree.version); err != nil {
				return 0, err
			}
		}

		b.logger.Debug().Msgf("db=tree orphans=%s", humanize.Comma(int64(len(tree.branchOrphans))))

		for _, orphan := range tree.branchOrphans {
			b.count++
			err = b.execBranchOrphan(orphan)
			if err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(tree.version); err != nil {
				return 0, err
			}
		}

		if err = b.treeBatchCommit(); err != nil {
			return 0, err
		}
		err = b.sql.treeWrite.Exec(fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", tree.version, tree.version))
		if err != nil {
			return 0, err
		}
	}

	return 0, nil
}
