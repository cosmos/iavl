package iavl

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type pruneResult struct {
	pruneTo int64
	err     error
}

func (sql *SqliteDb) beginPrune(pruneTo int64) {
	if sql.pruning {
		sql.logger.Warn().Int64("pruneTo", pruneTo).Msg("pruning already in progress")
	}
	sql.pruning = true
	sql.logger.Info().Int64("pruneTo", pruneTo).Msg("pruning")
	go func() {
		// TODO global pruning limit. e.g. 8
		err := sql.prune(pruneTo)
		sql.pruneCh <- &pruneResult{pruneTo: pruneTo, err: err}
	}()
}

func (sql *SqliteDb) prune(pruneTo int64) error {
	// create new pruned shard
	if err := sql.createTreeShardDb(pruneTo); err != nil {
		return err
	}
	// open new write connection to the pruned shard
	conn, err := sqlite3.Open(sql.opts.treeConnectionString(pruneTo))
	if err != nil {
		return err
	}
	err = conn.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}
	if err = conn.Exec("PRAGMA wal_autocheckpoint=-1"); err != nil {
		return err
	}

	for _, shard := range sql.shards.versions {
		if shard > pruneTo {
			break
		}
		if err := conn.Exec(fmt.Sprintf("ATTACH DATABASE ? AS shard_%d", shard), sql.opts.treeConnectionString(shard)); err != nil {
			return err
		}
		if err := sql.pruneShard(shard, pruneTo, conn, false); err != nil {
			return err
		}
		err = conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		if err != nil {
			return err
		}
		if err := sql.pruneShard(shard, pruneTo, conn, true); err != nil {
			return err
		}
		if err := conn.Exec(fmt.Sprintf("DETACH DATABASE shard_%d", shard)); err != nil {
			return err
		}
	}

	err = conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	if err != nil {
		return err
	}
	err = conn.Exec("CREATE INDEX tree_idx ON tree (version, sequence)")
	if err != nil {
		return err
	}
	err = conn.Exec("CREATE UNIQUE INDEX leaf_idx ON leaf (version, sequence)")
	if err != nil {
		return err
	}
	return conn.Close()
}

func (sql *SqliteDb) pruneShard(shardID, _ int64, conn *sqlite3.Conn, branches bool) error {
	join := make(map[int64]map[int64]bool)

	orphanQry := fmt.Sprintf("SELECT version, sequence FROM shard_%d.orphan", shardID)
	treeQry := fmt.Sprintf("SELECT version, sequence, bytes FROM shard_%d.tree", shardID)
	insertStmt := "INSERT INTO tree (version, sequence, bytes) VALUES (?, ?, ?)"
	if !branches {
		orphanQry = fmt.Sprintf("SELECT version, sequence FROM shard_%d.leaf_orphan", shardID)
		treeQry = fmt.Sprintf("SELECT version, sequence, bytes FROM shard_%d.leaf", shardID)
		insertStmt = "INSERT INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)"
	}

	q, err := conn.Prepare(orphanQry)
	if err != nil {
		return err
	}
	var version, sequence int64
	for {
		if hasRow, err := q.Step(); err != nil {
			return err
		} else if !hasRow {
			break
		}
		if err := q.Scan(&version, &sequence); err != nil {
			return err
		}
		if _, ok := join[version]; !ok {
			join[version] = make(map[int64]bool)
		}
		join[version][sequence] = true
	}
	if err := q.Close(); err != nil {
		return err
	}

	// insert orphans
	if err := conn.Begin(); err != nil {
		return err
	}
	q, err = conn.Prepare(treeQry)
	if err != nil {
		return err
	}
	insert, err := conn.Prepare(insertStmt)
	if err != nil {
		return err
	}

	var (
		bz []byte
		i  int
	)
	for {
		if hasRow, err := q.Step(); err != nil {
			return err
		} else if !hasRow {
			break
		}
		if err := q.Scan(&version, &sequence, &bz); err != nil {
			return err
		}
		if _, ok := join[version][sequence]; ok {
			continue
		}
		if err := insert.Exec(version, sequence, bz); err != nil {
			return err
		}
		i++
		if i%200_000 == 0 {
			if err := conn.Commit(); err != nil {
				return err
			}
			if err := conn.Begin(); err != nil {
				return err
			}
		}
	}

	if err := conn.Commit(); err != nil {
		return err
	}
	if err := q.Close(); err != nil {
		return err
	}
	if err := insert.Close(); err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) checkPruning() error {
	select {
	case res := <-sql.pruneCh:
		if res.err != nil {
			return res.err
		}
		if err := sql.hotConnectionFactory.addShard(res.pruneTo); err != nil {
			return err
		}
		newShards := &VersionRange{versions: []int64{res.pruneTo}}
		var dropped []int64
		for _, v := range sql.shards.versions {
			if v > res.pruneTo {
				err := newShards.Add(v)
				if err != nil {
					return err
				}
			} else {
				// TODO
				// maybe delay this here to wait for open read connections to stale shards to close
				if err := sql.hotConnectionFactory.removeShard(v); err != nil {
					return err
				}
				// delete shard files from disk
				path := fmt.Sprintf("%s/tree_%06d*", sql.opts.Path, v)

				matches, err := filepath.Glob(path)
				if err != nil {
					return err
				}
				for _, match := range matches {
					if err := os.Remove(match); err != nil {
						return err
					}
				}
				dropped = append(dropped, v)
			}
		}
		sql.logger.Info().Msgf("dropped %v", dropped)
		sql.shards = newShards
		sql.pruning = false
		// TODO update shards table
		return nil
	default:
		return nil
	}
}
