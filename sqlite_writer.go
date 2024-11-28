package iavl

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
)

type pruneSignal struct {
	pruneVersion int64
	checkpoints  VersionRange
}

type saveSignal struct {
	batch          *sqliteBatch
	root           *Node
	wantCheckpoint bool
}

type saveResult struct {
	n   int64
	err error
}

type sqlWriter struct {
	sql    *SqliteDb
	logger zerolog.Logger
	cache  map[NodeKey]*Node

	treePruneCh chan *pruneSignal
	treeCh      chan *saveSignal
	treeResult  chan *saveResult

	leafPruneCh chan *pruneSignal
	leafCh      chan *saveSignal
	leafResult  chan *saveResult

	saving atomic.Bool
}

func (sql *SqliteDb) newSQLWriter() *sqlWriter {
	return &sqlWriter{
		sql:         sql,
		leafPruneCh: make(chan *pruneSignal),
		treePruneCh: make(chan *pruneSignal),
		leafCh:      make(chan *saveSignal),
		treeCh:      make(chan *saveSignal),
		leafResult:  make(chan *saveResult),
		treeResult:  make(chan *saveResult),
		cache:       make(map[NodeKey]*Node),
		logger:      sql.logger.With().Str("module", "write").Logger(),
	}
}

func (w *sqlWriter) start(ctx context.Context) {
	go func() {
		err := w.treeLoopAsyncCommit(ctx)
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
		pruneVersion     int64
		nextPruneVersion int64
		checkpoints      VersionRange
		orphanQuery      *sqlite3.Stmt
		deleteOrphan     *sqlite3.Stmt
		deleteLeaf       *sqlite3.Stmt
		pruneCount       int64
		pruneStartTime   time.Time
		err              error
	)

	beginPruneBatch := func(pruneTo int64) error {
		if err = w.sql.leafWrite.Begin(); err != nil {
			return fmt.Errorf("failed to begin leaf prune tx; %w", err)
		}
		orphanQuery, err = w.sql.leafWrite.Prepare(`SELECT version, sequence, ROWID FROM leaf_orphan WHERE at <= ?`, pruneTo)
		if err != nil {
			return fmt.Errorf("failed to prepare leaf orphan query; %w", err)
		}
		deleteOrphan, err = w.sql.leafWrite.Prepare("DELETE FROM leaf_orphan WHERE ROWID = ?")
		if err != nil {
			return fmt.Errorf("failed to prepare leaf orphan delete; %w", err)
		}
		deleteLeaf, err = w.sql.leafWrite.Prepare("DELETE FROM leaf WHERE version = ? and sequence = ?")
		if err != nil {
			return fmt.Errorf("failed to prepare leaf delete; %w", err)
		}

		return nil
	}
	startPrune := func(startPruningVersion int64) error {
		// only prune leafs to shard (checkpoint) boundaries.
		// e.g. given shards = [100, 200, 300];
		// startPruningVersion = 150; pruneTo = 100
		// startPruningVersion = 350; pruneTo = 300
		// startPruningVersion = 50; do nothing
		pruneTo := checkpoints.FindPrevious(startPruningVersion)
		if pruneTo == -1 {
			w.logger.Debug().Msgf("skipping leaf prune: requested prune version %d < first checkpoint", startPruningVersion)
			return nil
		}
		pruneVersion = pruneTo
		pruneCount = 0
		pruneStartTime = time.Now()

		w.logger.Debug().Msgf("leaf prune starting requested=%d pruneTo=%d", startPruningVersion, pruneTo)
		if err = beginPruneBatch(pruneVersion); err != nil {
			return err
		}
		return nil
	}
	commitPrune := func() error {
		if err = orphanQuery.Close(); err != nil {
			return err
		}
		orphanQuery = nil
		if err = w.sql.leafWrite.Commit(); err != nil {
			return err
		}
		w.logger.Debug().Msgf("commit leaf prune count=%s", humanize.Comma(pruneCount))
		if err = w.sql.leafWrite.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
			return fmt.Errorf("failed to checkpoint; %w", err)
		}

		if err = deleteLeaf.Close(); err != nil {
			return err
		}
		if err = deleteOrphan.Close(); err != nil {
			return err
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
				rowID    int64
			)
			err = orphanQuery.Scan(&version, &sequence, &rowID)
			if err != nil {
				return err
			}
			if err = deleteLeaf.Exec(version, sequence); err != nil {
				return err
			}
			if err = deleteOrphan.Exec(rowID); err != nil {
				return err
			}
			if pruneCount%pruneBatchSize == 0 {
				if err = commitPrune(); err != nil {
					return err
				}
				if err = beginPruneBatch(pruneVersion); err != nil {
					return err
				}
			}
		} else {
			if err = commitPrune(); err != nil {
				return err
			}
			err = w.sql.leafWrite.Exec("DELETE FROM leaf_delete WHERE version < ?", pruneVersion)
			if err != nil {
				return fmt.Errorf("failed to prune leaf_delete; %w", err)
			}
			w.logger.Debug().Msgf("done leaf prune count=%s dur=%s to=%d",
				humanize.Comma(pruneCount),
				time.Since(pruneStartTime).Round(time.Millisecond),
				pruneVersion,
			)
			if nextPruneVersion != 0 {
				if err = startPrune(nextPruneVersion); err != nil {
					return err
				}
				nextPruneVersion = 0
			} else {
				pruneVersion = 0
			}
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
				if err = commitPrune(); err != nil {
					return fmt.Errorf("interrupt leaf prune failed in commit; %w", err)
				}
				saveLeaves(sig)
				if err = beginPruneBatch(pruneVersion); err != nil {
					return fmt.Errorf("interrupt leaf prune failed in begin; %w", err)
				}
			case sig := <-w.leafPruneCh:
				w.logger.Warn().Msgf("leaf prune signal received while pruning version=%d next=%d", pruneVersion, sig.pruneVersion)
				checkpoints = sig.checkpoints
				nextPruneVersion = sig.pruneVersion
			case <-ctx.Done():
				return nil
			default:
				err = stepPruning()
				if err != nil {
					return fmt.Errorf("failed to step pruning; %w", err)
				}
			}
		} else {
			select {
			case sig := <-w.leafCh:
				saveLeaves(sig)
			case sig := <-w.leafPruneCh:
				checkpoints = sig.checkpoints
				err = startPrune(sig.pruneVersion)
				if err != nil {
					return fmt.Errorf("failed to start leaf prune; %w", err)
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
		checkpoints      VersionRange
		pruneVersion     int64
		pruneCount       int64
		pruneStartTime   time.Time
		orphanQuery      *sqlite3.Stmt
		// TODO use a map
		deleteBranch func(shardId int64, version int64, sequence int) (err error)
		deleteOrphan *sqlite3.Stmt
	)
	beginPruneBatch := func(version int64) (err error) {
		if err = w.sql.treeWrite.Begin(); err != nil {
			return err
		}
		orphanQuery, err = w.sql.treeWrite.Prepare(
			"SELECT version, sequence, at, ROWID FROM orphan WHERE at <= ?", version)
		if err != nil {
			return fmt.Errorf("failed to prepare orphan query; %w", err)
		}
		deleteBranch = func(shardId int64, version int64, sequence int) (err error) {
			return w.sql.treeWrite.Exec(
				fmt.Sprintf("DELETE FROM tree_%d WHERE version = ? AND sequence = ?", shardId), version, sequence)
		}
		deleteOrphan, err = w.sql.treeWrite.Prepare("DELETE FROM orphan WHERE ROWID = ?")
		if err != nil {
			return fmt.Errorf("failed to prepare orphan delete; %w", err)
		}

		return err
	}
	commitPrune := func() (err error) {
		if err = orphanQuery.Close(); err != nil {
			return err
		}
		if err = deleteOrphan.Close(); err != nil {
			return err
		}
		if err = w.sql.treeWrite.Commit(); err != nil {
			return fmt.Errorf("failed to commit tree prune; %w", err)
		}
		w.logger.Debug().Msgf("commit tree prune count=%s", humanize.Comma(pruneCount))
		if err = w.sql.treeWrite.Exec("PRAGMA wal_checkpoint(RESTART)"); err != nil {
			return fmt.Errorf("failed to checkpoint; %w", err)
		}
		return nil
	}
	saveTree := func(sig *saveSignal) {
		err := w.sql.SaveRoot(sig.batch.version, sig.root, sig.wantCheckpoint)
		if err != nil {
			w.treeResult <- &saveResult{err: fmt.Errorf("failed to save root path=%s version=%d: %w", w.sql.opts.Path, sig.batch.version, err)}
			return
		}
		if !sig.batch.isCheckpoint() {
			w.treeResult <- &saveResult{n: 0}
			return
		}

		w.logger.Debug().Msgf("will save branches=%d", len(sig.batch.branches))
		w.saving.Store(true)
		w.treeResult <- &saveResult{n: -34}

		n, err := sig.batch.saveBranches()
		if err != nil {
			w.treeResult <- &saveResult{err: fmt.Errorf("failed to save branches; path=%s %w", w.sql.opts.Path, err)}
			w.saving.Store(false)
			return
		}
		if err := w.sql.treeWrite.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			w.treeResult <- &saveResult{err: fmt.Errorf("failed tree checkpoint; %w", err)}
			w.saving.Store(false)
		}
		w.logger.Debug().Msgf("tree save done branches=%d", n)
		w.saving.Store(false)
		w.treeResult <- &saveResult{n: n}
	}
	startPrune := func(startPruningVersion int64) error {
		w.logger.Debug().Msgf("tree prune to version=%d", startPruningVersion)
		pruneStartTime = time.Now()
		pruneCount = 0
		pruneVersion = startPruningVersion
		err := beginPruneBatch(pruneVersion)
		if err != nil {
			return err
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
				rowID    int64
			)
			err = orphanQuery.Scan(&version, &sequence, &at, &rowID)
			if err != nil {
				return err
			}
			shard, err := w.sql.getShard(version)
			if err != nil {
				return err
			}
			if err = deleteBranch(shard, version, sequence); err != nil {
				return fmt.Errorf("failed to delete from tree_%d count=%d; %w", shard, pruneCount, err)
			}
			if err = deleteOrphan.Exec(rowID); err != nil {
				return fmt.Errorf("failed to delete from orphan count=%d; %w", pruneCount, err)
			}
			if pruneCount%pruneBatchSize == 0 {
				if err = commitPrune(); err != nil {
					return err
				}
				if err = beginPruneBatch(pruneVersion); err != nil {
					return err
				}
			}
		} else {
			if err = commitPrune(); err != nil {
				return err
			}

			prevCheckpoint := checkpoints.FindPrevious(pruneVersion)
			if err = w.sql.treeWrite.Exec("DELETE FROM root WHERE version < ?", prevCheckpoint); err != nil {
				return fmt.Errorf("failed to prune root to %d; %w", prevCheckpoint, err)
			}

			w.logger.Debug().Msgf("done tree prune count=%s dur=%s to=%d",
				humanize.Comma(pruneCount),
				time.Since(pruneStartTime).Round(time.Millisecond),
				prevCheckpoint,
			)
			if nextPruneVersion != 0 {
				if err = startPrune(nextPruneVersion); err != nil {
					return err
				}
				nextPruneVersion = 0
			} else {
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
					if err := commitPrune(); err != nil {
						return err
					}
					saveTree(sig)
					if err := beginPruneBatch(pruneVersion); err != nil {
						return err
					}
				} else {
					saveTree(sig)
				}
			case sig := <-w.treePruneCh:
				w.logger.Warn().Msgf("tree prune signal received while pruning version=%d next=%d", pruneVersion, sig.pruneVersion)
				checkpoints = sig.checkpoints
				nextPruneVersion = sig.pruneVersion
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
				checkpoints = sig.checkpoints
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

func (w *sqlWriter) treeLoopAsyncCommit(ctx context.Context) error {
	for {
		select {
		case sig := <-w.treeCh:
			res := &saveResult{}
			res.n, res.err = w.saveCheckpoint(sig.batch, sig.root)
			w.treeResult <- res

		case <-ctx.Done():
			return nil
		}
	}
}

func (w *sqlWriter) saveCheckpoint(batch *sqliteBatch, root *Node) (int64, error) {
	n, err := batch.saveBranches()
	if err != nil {
		return n, fmt.Errorf("batch.saveBranches failed; %w", err)
	}
	if err := w.sql.treeWrite.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return n, fmt.Errorf("wal_checkpoint failed; %w", err)
	}
	err = w.sql.SaveRoot(batch.version, root, true)
	if err != nil {
		return n, fmt.Errorf("sql.SaveRoot failed node=%v; %w", root, err)
	}
	return n, err
}

func (w *sqlWriter) saveTree(tree *Tree) error {
	saveStart := time.Now()
	batch := &sqliteBatch{
		sql:  tree.sql,
		tree: tree,
		size: 200_000,
		logger: log.With().
			Str("module", "sqlite-batch").
			Str("path", tree.sql.opts.Path).Logger(),
		version: tree.version,
	}

	select {
	case treeRes := <-w.treeResult:
		if treeRes.err != nil {
			return fmt.Errorf("err from last checkpoint: %w", treeRes.err)
		}
		w.logger.Debug().Msgf("ACK last checkpoint branches=%d", treeRes.n)
		// TODO empty cache if successful result found

	default:
	}

	saveSig := &saveSignal{batch: batch, root: tree.pool.clone(tree.root), wantCheckpoint: tree.shouldCheckpoint}
	w.leafCh <- saveSig

	if tree.shouldCheckpoint {
		// nextCache := make(map[NodeKey]*Node)
		// TODO!
		// for caching, need a double copy.
		// 1) once to freeze state for save
		// 2) once to cache; if an evicted node is pulled from cache and mutated it will result in a bad write
		for _, branch := range tree.branches {
			batch.branches = append(batch.branches, tree.pool.clone(branch))
			// if branch.evict {
			// 	branch.leftNode = nil
			// 	branch.rightNode = nil
			// 	nextCache[branch.nodeKey] = branch
			// }
		}
		// if err := debugDump(tree.sql.opts.Path, tree.version, nextCache); err != nil {
		// 	return err
		// }

		// wait for prior save to complete
		startWait := time.Now()
		w.treeCh <- saveSig
		w.logger.Debug().Msgf("save signal sent, waited %s", time.Since(startWait).Round(time.Millisecond))

		// swap caches
		// for _, n := range w.cache {
		// 	tree.returnNode(n)
		// }
		// w.cache = nextCache

		// for _, node := range tree.branches {
		// 	if node.evict {
		// 		evictCount++
		// 	}
		// }
		// w.logger.Debug().Msgf("saving checkpoint version=%d branches=%d evict=%d",
		// 	tree.version, len(tree.branches), evictCount)
	}

	leafResult := <-w.leafResult
	dur := time.Since(saveStart)
	tree.sql.metrics.WriteDurations = append(tree.sql.metrics.WriteDurations, dur)
	tree.sql.metrics.WriteTime += dur
	tree.sql.metrics.WriteLeaves += int64(len(tree.leaves))

	return leafResult.err
}

func (w *sqlWriter) cachePop(key NodeKey) (*Node, bool) {
	n, ok := w.cache[key]
	if ok {
		delete(w.cache, key)
	}
	return n, ok
}

func debugDump(path string, version int64, cache map[NodeKey]*Node) error {
	module := strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
	if module == "slashing" || module == "lockup" {
		f, err := os.Create(fmt.Sprintf("%s-%d.txt", module, version))
		if err != nil {
			return err
		}
		defer f.Close()
		for k := range cache {
			_, err := f.WriteString(fmt.Sprintf("%s\n", k))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// TODO
// unify delete approach between tree and leaf. tree uses rowid range in delete, leaf issues delete for each rowid.
// which one is faster?
//
