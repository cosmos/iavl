package iavl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
)

type pruneSignal struct {
	pruneVersion int64
}

type saveSignal struct {
	batch          *sqliteBatch
	root           *Node
	version        int64
	wantCheckpoint bool
}

type saveResult struct {
	n   int64
	err error
}

type sqlWriter struct {
	sql    *SqliteDb
	logger zerolog.Logger

	treePruneCh chan *pruneSignal
	treeCh      chan *saveSignal
	treeResult  chan *saveResult

	leafPruneCh chan *pruneSignal
	leafCh      chan *saveSignal
	leafResult  chan *saveResult
}

func (sql *SqliteDb) newSqlWriter() *sqlWriter {
	return &sqlWriter{
		sql:         sql,
		leafPruneCh: make(chan *pruneSignal),
		treePruneCh: make(chan *pruneSignal),
		leafCh:      make(chan *saveSignal),
		treeCh:      make(chan *saveSignal),
		leafResult:  make(chan *saveResult),
		treeResult:  make(chan *saveResult),
		logger:      sql.logger.With().Str("module", "write").Logger(),
	}
}

func (w *sqlWriter) start(ctx context.Context) {
	go func() {
		err := w.treeLoop(ctx)
		if err != nil {
			w.logger.Fatal().Err(err).Msg("tree loop failed")
		}
	}()
	go func() {
		err := w.leafLoop(ctx)
		if err != nil {
			w.logger.Fatal().Err(err).Msg("leaf loop failed")
		}
	}()
}

func (w *sqlWriter) leafLoop(ctx context.Context) error {
	var (
		pruneVersion int64
		orphanQuery  *sqlite3.Stmt
		pruneCount   int64
		lastOrphanId int64
		deleteOrphan *sqlite3.Stmt
		deleteLeaf   *sqlite3.Stmt
		err          error
	)
	deleteOrphan, err = w.sql.leafWrite.Prepare("DELETE FROM leaf_orphan WHERE ROWID = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare leaf orphan delete; %w", err)
	}
	deleteLeaf, err = w.sql.leafWrite.Prepare("DELETE FROM leaf WHERE version = ? and sequence = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare leaf delete; %w", err)
	}
	resetOrphanQuery := func(startPruningVersion int64) error {
		if err = w.sql.leafWrite.Begin(); err != nil {
			return fmt.Errorf("failed to begin leaf prune tx; %w", err)
		}
		orphanQuery, err = w.sql.leafWrite.Prepare(`SELECT version, sequence, ROWID FROM leaf_orphan WHERE at <= ?`, startPruningVersion)
		if err != nil {
			return fmt.Errorf("failed to prepare leaf orphan query; %w", err)
		}
		return nil
	}
	startPrune := func(startPruningVersion int64) error {
		w.logger.Debug().Msgf("leaf pruning version=%d", startPruningVersion)
		// only prune leafs to shard (checkpoint) boundaries.
		pruneTo := w.sql.shards.Find(startPruningVersion)
		if pruneTo == -1 {
			pruneTo = w.sql.shards.Last()
			if pruneTo == -1 {
				return fmt.Errorf("prune: no shards found")
			}
		}
		pruneVersion = pruneTo
		if err = resetOrphanQuery(pruneVersion); err != nil {
			return err
		}
		return nil
	}
	commitOrphaned := func() error {
		if err = orphanQuery.Close(); err != nil {
			return err
		}
		if err = w.sql.leafWrite.Commit(); err != nil {
			return err
		}
		w.logger.Info().Msgf("commit leaf pruning count=%s", humanize.Comma(pruneCount))
		if err = w.sql.leafWrite.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
			return fmt.Errorf("failed to checkpoint; %w", err)
		}
		return nil
	}
	stepPruning := func() error {
		hasRow, err := orphanQuery.Step()
		if err != nil {
			return fmt.Errorf("failed to step leaf orphan query; %w", err)
		}
		if hasRow {
			pruneCount++
			var (
				version  int64
				sequence int
			)
			err = orphanQuery.Scan(&version, &sequence, &lastOrphanId)
			if err != nil {
				return err
			}
			if err = deleteLeaf.Exec(version, sequence); err != nil {
				return err
			}
			if err = deleteOrphan.Exec(lastOrphanId); err != nil {
				return err
			}
			if pruneCount%pruneBatchSize == 0 {
				if err = commitOrphaned(); err != nil {
					return err
				}
				if err = resetOrphanQuery(pruneVersion); err != nil {
					return err
				}
			}
		} else {
			if err = commitOrphaned(); err != nil {
				return err
			}
			orphanQuery = nil
			pruneVersion = 0
		}

		return nil
	}
	saveLeaves := func(sig *saveSignal) {
		res := &saveResult{}
		res.n, res.err = sig.batch.saveLeaves()
		if sig.batch.isCheckpoint() {
			if err = w.sql.leafWrite.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				w.logger.Err(err).Msg("failed leaf wal_checkpoint")
			}
		}
		w.leafResult <- res
	}
	for {
		if pruneVersion != 0 {
			select {
			case sig := <-w.leafCh:
				if err = commitOrphaned(); err != nil {
					return err
				}
				saveLeaves(sig)
				if err = startPrune(pruneVersion); err != nil {
					return err
				}
			case sig := <-w.leafPruneCh:
				w.logger.Fatal().Msgf("leaf prune signal received while pruning version=%d next=%d", pruneVersion, sig.pruneVersion)
			case <-ctx.Done():
				return nil
			default:
				err = stepPruning()
				if err != nil {
					return err
				}
			}
		} else {
			select {
			case sig := <-w.leafCh:
				saveLeaves(sig)
			case sig := <-w.leafPruneCh:
				err = startPrune(sig.pruneVersion)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return nil
			}
		}
	}
}

const pruneBatchSize = 500_000

func (w *sqlWriter) treeLoop(ctx context.Context) error {
	var (
		nextPruneVersion int64
		pruneVersion     int64
		pruneCount       int64
		pruneStartTime   time.Time
		orphanQuery      *sqlite3.Stmt
		// TODO use a map
		deleteOrphaned func(shardId int64, version int64, sequence int) (err error)
		lastOrphanId   int64
		startOrphanId  int64
	)
	resetOrphanQuery := func(version int64) (err error) {
		startOrphanId = lastOrphanId
		if err = w.sql.treeWrite.Begin(); err != nil {
			return err
		}
		orphanQuery, err = w.sql.treeWrite.Prepare(
			"SELECT version, sequence, at, rowid FROM orphan WHERE at <= ? AND rowid > ? ORDER BY at", version, startOrphanId)
		if err != nil {
			return fmt.Errorf("failed to prepare orphan query; %w", err)
		}
		deleteOrphaned = func(shardId int64, version int64, sequence int) (err error) {
			return w.sql.treeWrite.Exec(
				fmt.Sprintf("DELETE FROM tree_%d WHERE version = ? AND sequence = ?",
					shardId), version, sequence)
		}
		if err != nil {
			return fmt.Errorf("failed to prepare orphan delete; %w", err)
		}
		return err
	}
	commitOrphaned := func() (err error) {
		if err = orphanQuery.Close(); err != nil {
			return err
		}
		if err = w.sql.treeWrite.Exec("DELETE FROM orphan WHERE rowid >= ? AND rowid <= ?",
			startOrphanId, lastOrphanId); err != nil {
			return err
		}
		if err = w.sql.treeWrite.Commit(); err != nil {
			return err
		}
		w.logger.Info().Msgf("commit pruning count=%s low_rowid=%d high_rowid=%d",
			humanize.Comma(pruneCount), startOrphanId, lastOrphanId)
		if err = w.sql.treeWrite.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
			w.logger.Err(err).Msg("failed to checkpoint")
			return fmt.Errorf("failed to checkpoint; %w", err)
		}
		return nil
	}
	saveTree := func(sig *saveSignal) {
		res := &saveResult{}
		res.n, res.err = sig.batch.saveBranches()
		if res.err == nil {
			// TODO
			// SaveRoot should run even when not checkpointing
			err := w.sql.SaveRoot(sig.version, sig.root, sig.wantCheckpoint)
			if err != nil {
				res.err = fmt.Errorf("failed to save root path=%s version=%d: %w", w.sql.opts.Path, sig.version, err)
			}
		}
		if sig.batch.isCheckpoint() {
			w.logger.Info().Msgf("checkpointed version=%d count=%s", sig.version, humanize.Comma(res.n))
			if err := w.sql.treeWrite.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				return
			}
		}
		w.treeResult <- res
	}
	startPrune := func(startPruningVersion int64) error {
		w.logger.Info().Msgf("pruning version=%d", startPruningVersion)
		if orphanQuery != nil {
			w.logger.Warn().Msgf("prune signal received while pruning version=%d next=%d", pruneVersion, startPruningVersion)
			nextPruneVersion = startPruningVersion
		} else {
			pruneStartTime = time.Now()
			var err error
			pruneVersion = startPruningVersion
			err = resetOrphanQuery(pruneVersion)
			if err != nil {
				return err
			}
		}
		return nil
	}
	stepPruning := func() error {
		hasRow, err := orphanQuery.Step()
		if err != nil {
			return fmt.Errorf("failed to step orphan query; %w", err)
		}
		if hasRow {
			pruneCount++
			var (
				version  int64
				sequence int
				at       int
			)
			err = orphanQuery.Scan(&version, &sequence, &at, &lastOrphanId)
			if err != nil {
				return err
			}
			cv := w.sql.shards.FindMemoized(version)
			if cv == -1 {
				return fmt.Errorf("prune: checkpoint for version %d not found", version)
			}
			if err = deleteOrphaned(cv, version, sequence); err != nil {
				return fmt.Errorf("failed to delete from tree_%d count=%d; %w", cv, pruneCount, err)
			}
			if pruneCount%pruneBatchSize == 0 {
				if err = commitOrphaned(); err != nil {
					return err
				}
				if err = resetOrphanQuery(pruneVersion); err != nil {
					return err
				}
			}
		} else {
			if err = commitOrphaned(); err != nil {
				return err
			}

			w.logger.Info().Msgf("done pruning count=%s dur=%s",
				humanize.Comma(pruneCount),
				time.Since(pruneStartTime).Round(time.Millisecond),
			)
			pruneCount = 0
			if nextPruneVersion != 0 {
				pruneStartTime = time.Now()
				if err = resetOrphanQuery(nextPruneVersion); err != nil {
					return err
				}
				pruneVersion = nextPruneVersion
				nextPruneVersion = 0
			} else {
				orphanQuery = nil
				pruneVersion = 0
			}
		}

		return nil
	}

	for {
		// if there is pruning in progress support interrupt and immediate continuation
		if pruneVersion != 0 {
			select {
			case sig := <-w.treeCh:
				if sig.wantCheckpoint {
					if err := commitOrphaned(); err != nil {
						return err
					}
					orphanQuery = nil
					saveTree(sig)
					if err := startPrune(pruneVersion); err != nil {
						return err
					}
				} else {
					saveTree(sig)
				}
			case sig := <-w.treePruneCh:
				err := startPrune(sig.pruneVersion)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return nil
			default:
				// continue pruning if no signal
				err := stepPruning()
				if err != nil {
					return err
				}
			}
		} else {
			select {
			case sig := <-w.treeCh:
				saveTree(sig)
			case sig := <-w.treePruneCh:
				err := startPrune(sig.pruneVersion)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return nil
			}
		}
	}

}

func (w *sqlWriter) saveTree(tree *Tree) error {
	batch := &sqliteBatch{
		sql:  tree.sql,
		tree: tree,
		size: 200_000,
		logger: log.With().
			Str("module", "sqlite-batch").
			Str("path", tree.sql.opts.Path).Logger(),
	}
	saveSig := &saveSignal{batch: batch, root: tree.root, version: tree.version, wantCheckpoint: tree.shouldCheckpoint}
	if tree.shouldCheckpoint {
		w.logger.Info().Msgf("checkpointing version=%d path=%s", tree.version, tree.sql.opts.Path)
	}
	w.treeCh <- saveSig
	w.leafCh <- saveSig
	treeResult := <-w.treeResult
	leafResult := <-w.leafResult
	err := errors.Join(treeResult.err, leafResult.err)

	return err
}
