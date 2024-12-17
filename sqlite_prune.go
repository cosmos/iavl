package iavl

import "github.com/bvinc/go-sqlite-lite/sqlite3"

// 1) checkpoint into previous shard
// 2) create new shard
// 2a) next version leaves accumulate into new shard
// 3) in background, create new most recent shard by copy un-orphaned leaves/branches into one new shard

func (sql *SqliteDb) beginPrune(treeVersion int64, pruneTo int64) error {
	// TODO thread-safe coordination, one prune at a time.

	pruneVersion := sql.shards.FindPrevious(pruneTo)
	shards := make([]int64, len(sql.shards.versions))
	copy(shards, sql.shards.versions)

	if err := sql.createTreeShardDb(treeVersion + 1); err != nil {
		return err
	}
	if err := sql.createTreeShardDb(pruneVersion); err != nil {
		return err
	}
	sql.logger.Info().Int64("pruneVersion", pruneVersion).Int64("pruneTo", pruneTo).Msg("beginPrune")

	for _, shard := range shards {
		if shard > pruneVersion {
			break
		}
		read, err := sqlite3.Open(sql.opts.treeConnectionString(shard))
		if err != nil {
			return err
		}

		if err = read.Close(); err != nil {
			return err
		}
	}

	return nil
}
