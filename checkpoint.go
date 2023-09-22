package iavl

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
)

type checkpointArgs struct {
	set     []*Node
	delete  [][]byte
	version int64
}

type checkpointer struct {
	db       *kvDB
	sqliteDb *sqliteDb
	cache    *NodeCache
	ch       chan *checkpointArgs
	pool     *nodePool
}

func newCheckpointer(db *kvDB, cache *NodeCache, pool *nodePool) *checkpointer {
	return &checkpointer{
		db:    db,
		cache: cache,
		ch:    make(chan *checkpointArgs),
		pool:  pool,
	}
}

func (cp *checkpointer) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case args := <-cp.ch:
			start := time.Now()
			log.Info().Msgf("checkpoint start ver=%d", args.version)

			var memSize, dbSize uint64

			sort.Slice(args.set, func(i, j int) bool {
				a := args.set[i]
				b := args.set[j]
				if a.nodeKey.version != b.nodeKey.version {
					return a.nodeKey.version < b.nodeKey.version
				}
				return a.nodeKey.sequence < b.nodeKey.sequence
			})

			sort.Slice(args.delete, func(i, j int) bool {
				return bytes.Compare(args.delete[i], args.delete[j]) < 0
			})

			for _, nodeKey := range args.delete {
				err := cp.db.Delete(nodeKey)
				if err != nil {
					return err
				}
			}

			for _, node := range args.set {
				memSize += node.sizeBytes()
				n, err := cp.db.Set(node)
				if err != nil {
					return err
				}

				dbSize += uint64(n)
				cp.pool.Put(node)
			}

			log.Info().Msgf("checkpoint done ver=%d dur=%s set=%s del=%s mem_sz=%s db_sz=%s",
				args.version,
				time.Since(start).Round(time.Millisecond),
				humanize.Comma(int64(len(args.set))),
				humanize.Comma(int64(len(args.delete))),
				humanize.IBytes(memSize),
				humanize.IBytes(dbSize),
			)
		}
	}
}
