package iavl

import (
	"context"
	"sort"
	"time"
)

type checkpointArgs struct {
	set     []*Node
	delete  [][]byte
	version int64
}

type checkpointer struct {
	db    *kvDB
	cache *NodeCache
	ch    chan *checkpointArgs
}

func newCheckpointer(db *kvDB, cache *NodeCache) *checkpointer {
	return &checkpointer{
		db:    db,
		cache: cache,
		ch:    make(chan *checkpointArgs),
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

			sort.Slice(args.set, func(i, j int) bool {
				a := args.set[i]
				b := args.set[j]
				if a.nodeKey.version != b.nodeKey.version {
					return a.nodeKey.version < b.nodeKey.version
				}
				return a.nodeKey.sequence < b.nodeKey.sequence
			})

			for _, node := range args.set {
				_, err := cp.db.Set(node)
				if err != nil {
					return err
				}
			}

			//for _, nodeKey := range args.delete {
			//	err := cp.db.Delete(nodeKey)
			//	if err != nil {
			//		return err
			//	}
			//}

			log.Info().Msgf("checkpoint done ver=%d dur=%s",
				args.version,
				time.Since(start).Round(time.Millisecond))
		}
	}
}
