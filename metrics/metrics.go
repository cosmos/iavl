package metrics

import (
	"fmt"

	"github.com/dustin/go-humanize"
)

type TreeMetrics struct {
	PoolGet       int64
	PoolReturn    int64
	PoolEvict     int64
	PoolEvictMiss int64
	PoolFault     int64

	TreeUpdate        int64
	TreeNewNode       int64
	TreeDelete        int64
	PoolDirtyOverflow int64
}

func (m *TreeMetrics) Report() {
	fmt.Printf("Pool:\n gets: %s, returns: %s, faults: %s, evicts: %s, evict miss %s, dirty overflow: %s\n",
		humanize.Comma(m.PoolGet),
		humanize.Comma(m.PoolReturn),
		humanize.Comma(m.PoolFault),
		humanize.Comma(m.PoolEvict),
		humanize.Comma(m.PoolEvictMiss),
		humanize.Comma(m.PoolDirtyOverflow))

	fmt.Printf("\nTree:\n update: %s, new node: %s, delete: %s\n",
		humanize.Comma(m.TreeUpdate),
		humanize.Comma(m.TreeNewNode),
		humanize.Comma(m.TreeDelete))
}
