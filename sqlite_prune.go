package iavl

import (
	"fmt"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

var globalPruneLimit chan struct{}

func init() {
	SetGlobalPruneLimit(2)
}

func SetGlobalPruneLimit(n int) {
	globalPruneLimit = make(chan struct{}, n)
	for i := 0; i < n; i++ {
		globalPruneLimit <- struct{}{}
	}
}

// TODO:
// count leaf orphans (low priority)
// global pruning limit (e.g. 2)
// ensure orphan join is correct... should it be across all shards or just the current shard?
//  - pretty sure we need all shards
// consider bringing back at index on orphans and chunking to reduce gc pressure like:
//  - select * from orphan order by at asc
//  - accumulate until n rows
//  - process inserts
//  - repeat until done

type pruneResult struct {
	pruneTo int64
	took    time.Duration
	wait    time.Duration
	err     error
}

type joinTable map[int64]map[int64]bool

func (sql *SqliteDb) beginPrune(pruneTo int64) {
	if sql.pruning {
		sql.logger.Warn().Int64("pruneTo", pruneTo).Msg("pruning already in progress")
	}
	sql.pruning = true
	go func() {
		start := time.Now()
		lock := <-globalPruneLimit
		wait := time.Since(start)
		if wait > time.Second*10 {
			sql.logger.Warn().Str("waited", wait.String()).Msg("prune")
		}
		start = time.Now()
		err := sql.prune(pruneTo)
		took := time.Since(start)
		globalPruneLimit <- lock
		sql.pruneCh <- &pruneResult{pruneTo: pruneTo, err: err, took: took, wait: wait}
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

	// collect shards
	pruneShards := make([]int64, 0, len(sql.shards.versions))
	for _, shard := range sql.shards.versions {
		if shard > pruneTo {
			break
		}
		if err := conn.Exec(fmt.Sprintf("ATTACH DATABASE ? AS shard_%d", shard), sql.opts.treeConnectionString(shard)); err != nil {
			return err
		}
		pruneShards = append(pruneShards, shard)
	}

	sql.logger.Debug().Int64("pruneTo", pruneTo).Msgf("prune shards=%v", pruneShards)

	// prune branches
	join, err := sql.orphanJoins(conn, pruneShards, false)
	if err != nil {
		return err
	}
	for _, shard := range pruneShards {
		if err := sql.pruneShard(shard, conn, join, false); err != nil {
			return err
		}
		err = conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		if err != nil {
			return err
		}
	}

	// prune leaves
	join, err = sql.orphanJoins(conn, pruneShards, true)
	if err != nil {
		return err
	}
	for _, shard := range pruneShards {
		if err := sql.pruneShard(shard, conn, join, true); err != nil {
			return err
		}
		err = conn.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		if err != nil {
			return err
		}
	}

	err = conn.Exec("CREATE INDEX tree_idx ON tree (version, sequence)")
	if err != nil {
		return err
	}
	err = conn.Exec("CREATE UNIQUE INDEX leaf_idx ON leaf (version, sequence)")
	if err != nil {
		return err
	}

	// probably unnecessary
	for _, shard := range pruneShards {
		if err := conn.Exec(fmt.Sprintf("DETACH DATABASE shard_%d", shard)); err != nil {
			return err
		}
	}
	return conn.Close()
}

func (sql *SqliteDb) pruneShard(
	shardID int64, conn *sqlite3.Conn, join joinTable, leaves bool,
) error {
	treeQry := fmt.Sprintf("SELECT version, sequence, bytes FROM shard_%d.tree", shardID)
	insertStmt := "INSERT INTO tree (version, sequence, bytes) VALUES (?, ?, ?)"
	if leaves {
		treeQry = fmt.Sprintf("SELECT version, sequence, bytes FROM shard_%d.leaf", shardID)
		insertStmt = "INSERT INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)"
	}

	// insert orphans
	if err := conn.Begin(); err != nil {
		return err
	}
	q, err := conn.Prepare(treeQry)
	if err != nil {
		return err
	}
	insert, err := conn.Prepare(insertStmt)
	if err != nil {
		return err
	}

	var (
		bz                []byte
		i                 int
		version, sequence int64
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
			// maybe save some memory? should be 1:1
			// delete(join[version], sequence)
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

func (sql *SqliteDb) orphanJoins(conn *sqlite3.Conn, shards []int64, leaves bool) (joinTable, error) {
	var (
		join  = joinTable{}
		count int
		start = time.Now()
	)
	orphanQry := "SELECT version, sequence FROM shard_%d.orphan"
	if leaves {
		orphanQry = "SELECT version, sequence FROM shard_%d.leaf_orphan"
	}
	for _, shard := range shards {
		q, err := conn.Prepare(fmt.Sprintf(orphanQry, shard))
		if err != nil {
			return nil, err
		}
		for {
			if hasRow, err := q.Step(); err != nil {
				return nil, err
			} else if !hasRow {
				break
			}
			count++
			var version, sequence int64
			if err := q.Scan(&version, &sequence); err != nil {
				return nil, err
			}
			if _, ok := join[version]; !ok {
				join[version] = make(map[int64]bool)
			}
			join[version][sequence] = true
		}
		if err := q.Close(); err != nil {
			return nil, err
		}
	}
	sql.logger.Debug().
		Int("count", count).
		Str("dur", time.Since(start).String()).
		Msgf("orphan join")
	return join, nil
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
				// path := fmt.Sprintf("%s/tree_%06d*", sql.opts.Path, v)

				// matches, err := filepath.Glob(path)
				// if err != nil {
				// 	return err
				// }
				// for _, match := range matches {
				// 	if err := os.Remove(match); err != nil {
				// 		return err
				// 	}
				// }
				dropped = append(dropped, v)
			}
		}
		sql.logger.Info().
			Str("took", res.took.String()).
			Str("wait", res.wait.String()).
			Ints64("dropped", dropped).
			Msg("prune completed")
		sql.shards = newShards
		sql.pruning = false
		// TODO update shards table
		return nil
	default:
		return nil
	}
}
