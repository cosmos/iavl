package iavl

import (
	"fmt"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

// 1) checkpoint into previous shard
// 2) create new shard
// 2a) next version leaves accumulate into new shard
// 3) in background, create new most recent shard by copy un-orphaned leaves/branches into one new shard

// A shard is only pruned when the orphaned percentage exceeds a threshold, 0.5 by default.

func (sql *SqliteDb) beginPrune(pruneTo, treeVersion int64, checkpoints *VersionRange) error {
	// TODO thread-safe coordination, one prune at a time.

	pruneVersion := checkpoints.FindPrevious(pruneTo)
	if pruneVersion == -1 || pruneVersion == checkpoints.First() {
		sql.logger.Info().Int64("pruneVersion", pruneVersion).Int64("pruneTo", pruneTo).Msg("no pruning required")
		return nil
	}

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

	// create new pruned shard
	if err := sql.createTreeShardDb(pruneVersion); err != nil {
		return err
	}

	// TODO return control to calling thread here.

	// open new write connection to the pruned shard
	conn, err := sqlite3.Open(sql.opts.treeConnectionString(pruneVersion))
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

	sql.logger.Info().Int64("pruneVersion", pruneVersion).Int64("pruneTo", pruneTo).Msg("beginPrune")

	var (
		dropped []int64
		pivot   int
	)

	for i, shard := range sql.shards.versions {
		if shard > pruneVersion {
			pivot = i
			break
		}
		sql.logger.Info().Int64("shard", shard).Msg("create index")
		shardConn, err := sql.newWriteConnection(shard)
		if err != nil {
			return err
		}
		err = shardConn.Exec("CREATE INDEX leaf_orphan_idx ON leaf_orphan (version, sequence)")
		if err != nil {
			return err
		}
		err = shardConn.Exec("CREATE INDEX branch_orphan_idx ON orphan (version, sequence)")
		if err != nil {
			return err
		}
		if err = shardConn.Close(); err != nil {
			return err
		}

		err = conn.Exec(fmt.Sprintf("ATTACH DATABASE ? AS shard_%d", shard), sql.opts.treeConnectionString(shard))
		if err != nil {
			return err
		}
		err = conn.Exec(fmt.Sprintf(`
INSERT INTO main.leaf SELECT l.* FROM shard_%d.leaf as l
LEFT JOIN shard_%d.leaf_orphan o 
ON l.version = o.version AND l.sequence = o.sequence
WHERE o.version IS NULL;`, shard, shard))
		if err != nil {
			return err
		}
		err = conn.Exec(fmt.Sprintf(`
INSERT INTO main.tree SELECT t.* FROM shard_%d.tree as t
LEFT JOIN shard_%d.orphan o 
ON t.version = o.version AND t.sequence = o.sequence
WHERE o.version IS NULL;`, shard, shard))
		if err != nil {
			return err
		}
		err = conn.Exec("CREATE INDEX tree_idx ON tree (version, sequence)")
		if err != nil {
			return err
		}
		err = conn.Exec("CREATE INDEX leaf_idx ON leaf (version, sequence)")
		if err != nil {
			return err
		}
		dropped = append(dropped, shard)
	}

	sql.logger.Info().Msgf("dropped %v", dropped)
	for _, v := range dropped {
		if err := sql.hotConnectionFactory.removeShard(v); err != nil {
			return err
		}
	}

	newShards := []int64{pruneVersion}
	newShards = append(newShards, sql.shards.versions[pivot:]...)

	// TODO
	sql.shards = &VersionRange{versions: newShards}

	if err := sql.hotConnectionFactory.addShard(pruneVersion); err != nil {
		return err
	}
	if _, err := sql.resetWriteConnection(); err != nil {
		return err
	}

	if err = conn.Close(); err != nil {
		return err
	}

	return nil
}
