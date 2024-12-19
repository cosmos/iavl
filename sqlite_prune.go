package iavl

import (
	"fmt"
	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type pruneResult struct {
	pruneVersion int64
	dropped      []int64
	newShards    *VersionRange
	err          error
}

func (sql *SqliteDb) beginPrune(pruneTo, treeVersion int64, checkpoints *VersionRange) error {
	pruneVersion := checkpoints.FindPrevious(pruneTo)
	if pruneVersion == -1 || pruneVersion == checkpoints.First() {
		sql.logger.Info().Int64("pruneVersion", pruneVersion).Int64("pruneTo", pruneTo).Msg("no pruning required")
		return nil
	}

	if sql.pruning {
		sql.logger.Warn().Int64("pruneTo", pruneTo).Msg("pruning already in progress")
		return nil
	}
	sql.pruning = true

	// create next hot shard iff pruning will occur in current hot shard
	// but pruning rules require this to be so... so always create next hot shard.
	nextShard := treeVersion + 1
	if err := sql.createTreeShardDb(nextShard); err != nil {
		return err
	}
	// add next shard to index and factory
	if err := sql.shards.Add(nextShard); err != nil {
		return err
	}
	if err := sql.hotConnectionFactory.addShard(nextShard); err != nil {
		return err
	}
	if _, err := sql.resetWriteConnection(); err != nil {
		return err
	}

	sql.logger.Info().Int64("pruneVersion", pruneVersion).Int64("treeVersion", treeVersion).Msg("prune")
	go func() {
		dropped, newShards, err := sql.prune(pruneVersion)
		sql.pruneCh <- &pruneResult{pruneVersion: pruneVersion, err: err, dropped: dropped, newShards: newShards}
	}()
	return nil
}

func (sql *SqliteDb) prune(pruneVersion int64) (dropped []int64, newShards *VersionRange, err error) {
	// create new pruned shard
	if err = sql.createTreeShardDb(pruneVersion); err != nil {
		return dropped, newShards, err
	}
	// open new write connection to the pruned shard
	conn, err := sqlite3.Open(sql.opts.treeConnectionString(pruneVersion))
	if err != nil {
		return dropped, newShards, err
	}
	err = conn.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return dropped, newShards, err
	}
	if err = conn.Exec("PRAGMA wal_autocheckpoint=-1"); err != nil {
		return dropped, newShards, err
	}

	var (
		pivot     int
		shardConn *sqlite3.Conn
	)

	for i, shard := range sql.shards.versions {
		if shard > pruneVersion {
			pivot = i
			break
		}
		shardConn, err = sql.newWriteConnection(shard)
		if err != nil {
			return dropped, newShards, err
		}
		err = shardConn.Exec("CREATE INDEX leaf_orphan_idx ON leaf_orphan (version, sequence)")
		if err != nil {
			return dropped, newShards, err
		}
		err = shardConn.Exec("CREATE INDEX branch_orphan_idx ON orphan (version, sequence)")
		if err != nil {
			return dropped, newShards, err
		}
		if err = shardConn.Close(); err != nil {
			return dropped, newShards, err
		}

		err = conn.Exec(fmt.Sprintf("ATTACH DATABASE ? AS shard_%d", shard), sql.opts.treeConnectionString(shard))
		if err != nil {
			return dropped, newShards, err
		}
		err = conn.Exec(fmt.Sprintf(`
INSERT INTO main.leaf SELECT l.* FROM shard_%d.leaf as l
LEFT JOIN shard_%d.leaf_orphan o 
ON l.version = o.version AND l.sequence = o.sequence
WHERE o.version IS NULL;`, shard, shard))
		if err != nil {
			return dropped, newShards, err
		}
		err = conn.Exec(fmt.Sprintf(`
INSERT INTO main.tree SELECT t.* FROM shard_%d.tree as t
LEFT JOIN shard_%d.orphan o 
ON t.version = o.version AND t.sequence = o.sequence
WHERE o.version IS NULL;`, shard, shard))
		if err != nil {
			return dropped, newShards, err
		}
		dropped = append(dropped, shard)
	}

	err = conn.Exec("CREATE INDEX tree_idx ON tree (version, sequence)")
	if err != nil {
		return dropped, newShards, err
	}
	err = conn.Exec("CREATE UNIQUE INDEX leaf_idx ON leaf (version, sequence)")
	if err != nil {
		return dropped, newShards, err
	}

	newVersions := []int64{pruneVersion}
	newVersions = append(newVersions, sql.shards.versions[pivot:]...)
	newShards = &VersionRange{versions: newVersions}

	err = conn.Close()
	return dropped, newShards, err
}

func (sql *SqliteDb) checkPruning() error {
	select {
	case res := <-sql.pruneCh:
		if res.err != nil {
			return res.err
		}
		if err := sql.hotConnectionFactory.addShard(res.pruneVersion); err != nil {
			return err
		}
		sql.logger.Info().Msgf("dropping %v", res.dropped)
		for _, v := range res.dropped {
			if err := sql.hotConnectionFactory.removeShard(v); err != nil {
				return err
			}
			// TODO delete shard files from disk
		}
		sql.shards = res.newShards
		sql.pruning = false
		return nil
	default:
		return nil
	}
}
