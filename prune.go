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

	pruneCh    chan *pruneSignal
	treeCh     chan *saveSignal
	leafCh     chan *saveSignal
	leafResult chan *saveResult
	treeResult chan *saveResult
}

func (sql *SqliteDb) newSqlWriter() *sqlWriter {
	return &sqlWriter{
		sql:        sql,
		pruneCh:    make(chan *pruneSignal),
		leafCh:     make(chan *saveSignal),
		treeCh:     make(chan *saveSignal),
		leafResult: make(chan *saveResult),
		treeResult: make(chan *saveResult),
		logger:     sql.logger.With().Str("module", "write").Logger(),
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
	for {
		// TODO leaf prune
		select {
		case sig := <-w.leafCh:
			res := &saveResult{}
			res.n, res.err = sig.batch.saveLeaves()
			w.leafResult <- res
		case <-ctx.Done():
			return nil
			//default:
			//time.Sleep(10 * time.Microsecond)
			// prune
		}
	}
}

func (w *sqlWriter) treeLoop(ctx context.Context) error {
	var (
		nextPruneVersion int64
		pruneVersion     int64
		orphanQuery      *sqlite3.Stmt
		pruneCount       int64
		pruneTick        = time.NewTicker(10 * time.Microsecond)
	)
	pruneTick.Stop()
	saveTree := func(sig *saveSignal) {
		res := &saveResult{}
		res.n, res.err = sig.batch.saveBranches()
		if res.err == nil {
			err := w.sql.SaveRoot(sig.version, sig.root, sig.wantCheckpoint)
			if err != nil {
				res.err = fmt.Errorf("failed to save root path=%s version=%d: %w", w.sql.opts.Path, sig.version, err)
			}
		}
		w.treeResult <- res
		if sig.batch.isCheckpoint() {
			w.logger.Info().Msgf("checkpointed version=%d count=%s", sig.version, humanize.Comma(res.n))
		}
	}
	startPrune := func(sig *pruneSignal) error {
		w.logger.Info().Msgf("pruning version=%d", sig.pruneVersion)
		if orphanQuery != nil {
			w.logger.Warn().Msgf("prune signal received while pruning version=%d next=%d", pruneVersion, nextPruneVersion)
			nextPruneVersion = sig.pruneVersion
		} else {
			var err error
			pruneVersion = sig.pruneVersion
			orphanQuery, err = w.sql.treeWrite.Prepare("SELECT version, sequence, at FROM orphan WHERE at <= ? ORDER BY at", pruneVersion)
			if err != nil {
				return fmt.Errorf("failed to prepare orphan query; %w", err)
			}
		}
		pruneTick.Reset(10 * time.Microsecond)
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
				lastAt   int
			)
			err = orphanQuery.Scan(&version, &sequence, &at)
			if err != nil {
				return err
			}
			if at > lastAt {
				lastAt = at
				//w.logger.Info().Msgf("pruning version=%d", version)
			}
			cv := w.sql.shards.FindMemoized(version)
			if cv == -1 {
				return fmt.Errorf("checkpont for version %d not found", version)
			}
			if err := w.sql.treeWrite.Exec(fmt.Sprintf("DELETE FROM tree_%d WHERE version = ? AND sequence = ?", cv), version, sequence); err != nil {
				return fmt.Errorf("failed to delete from tree_%d count=%d; %w", cv, pruneCount, err)
			}
		} else {
			if err = orphanQuery.Close(); err != nil {
				return err
			}
			orphanDeleteStart := time.Now()
			if err = w.sql.treeWrite.Exec("DELETE FROM orphan WHERE at <= ?", pruneVersion); err != nil {
				return err
			}
			orphanDeleteDur := time.Since(orphanDeleteStart)
			w.logger.Info().Msgf("done pruning count=%s delete-dur=%s", humanize.Comma(pruneCount), orphanDeleteDur)
			pruneCount = 0
			if nextPruneVersion != 0 {
				var err error
				orphanQuery, err = w.sql.treeWrite.Prepare("SELECT * FROM orphan WHERE at <= ?", pruneVersion)
				if err != nil {
					return fmt.Errorf("failed to prepare orphan query; %w", err)
				}
				pruneVersion = nextPruneVersion
				nextPruneVersion = 0
			} else {
				pruneTick.Stop()
				orphanQuery = nil
				pruneVersion = 0
			}
		}

		return nil
	}

	for {
		// if there is pruning in progress support interrupt and immediate continuation
		if orphanQuery != nil {
			select {
			case sig := <-w.treeCh:
				saveTree(sig)
			case sig := <-w.pruneCh:
				err := startPrune(sig)
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
			case sig := <-w.pruneCh:
				err := startPrune(sig)
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
	w.treeCh <- saveSig
	w.leafCh <- saveSig
	treeResult := <-w.treeResult
	leafResult := <-w.leafResult
	err := errors.Join(treeResult.err, leafResult.err)

	//_, leafErr := batch.saveLeaves()
	//err := errors.Join(treeResult.err, leafErr)

	return err
}
