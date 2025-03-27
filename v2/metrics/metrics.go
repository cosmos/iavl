package metrics

import (
	"fmt"
	"os"
	"time"

	"github.com/aybabtme/uniplot/histogram"
	"github.com/dustin/go-humanize"
)

type Label struct {
	Name  string
	Value string
}

type Proxy interface {
	IncrCounter(val float32, keys ...string)
	SetGauge(val float32, keys ...string)
	MeasureSince(start time.Time, keys ...string)
}

var (
	_ Proxy = &StructMetrics{}
	_ Proxy = &NilMetrics{}
)

type NilMetrics struct{}

func (n NilMetrics) IncrCounter(_ float32, _ ...string) {}

func (n NilMetrics) SetGauge(_ float32, _ ...string) {}

func (n NilMetrics) MeasureSince(_ time.Time, _ ...string) {}

type StructMetrics struct {
	*TreeMetrics
	*DbMetrics
}

func NewStructMetrics() *StructMetrics {
	return &StructMetrics{
		TreeMetrics: &TreeMetrics{},
		DbMetrics:   &DbMetrics{},
	}
}

func (s *StructMetrics) IncrCounter(val float32, keys ...string) {
	if len(keys) != 2 {
		return
	}
	k := keys[1]
	switch k {
	case "pool_get":
		s.PoolGet += int64(val)
	case "pool_return":
		s.PoolReturn += int64(val)
	case "pool_evict":
		s.PoolEvict += int64(val)
	case "pool_evict_miss":
		s.PoolEvictMiss += int64(val)
	case "pool_fault":
		s.PoolFault += int64(val)

	case "tree_update":
		s.TreeUpdate += int64(val)
	case "tree_new_node":
		s.TreeNewNode += int64(val)
	case "tree_delete":
		s.TreeDelete += int64(val)
	case "tree_hash":
		s.TreeHash += int64(val)

	case "db_get_leaf":
		s.QueryLeafCount += int64(val)
	case "db_get_branch":
		s.QueryBranchCount += int64(val)
	case "db_leaf_miss":
		s.QueryLeafMiss += int64(val)
	case "db_write_leaf":
		s.WriteLeaves += int64(val)
	case "db_write_branch":
		s.WriteBranch += int64(val)
	}
}

func (s *StructMetrics) SetGauge(_ float32, _ ...string) {}

func (s *StructMetrics) MeasureSince(start time.Time, keys ...string) {
	dur := time.Since(start)
	if len(keys) != 2 {
		return
	}
	k := keys[1]
	switch k {
	case "db_get":
		s.QueryDurations = append(s.QueryDurations, dur)
		s.QueryTime += dur
		s.QueryCount++
	case "db_write":
		s.WriteDurations = append(s.WriteDurations, dur)
		s.WriteTime += dur
	}
}

type TreeMetrics struct {
	PoolGet       int64
	PoolReturn    int64
	PoolEvict     int64
	PoolEvictMiss int64
	PoolFault     int64

	TreeUpdate  int64
	TreeNewNode int64
	TreeDelete  int64
	TreeHash    int64
}

type DbMetrics struct {
	WriteDurations []time.Duration
	WriteTime      time.Duration
	WriteLeaves    int64
	WriteBranch    int64

	QueryDurations   []time.Duration
	QueryTime        time.Duration
	QueryCount       int64
	QueryLeafMiss    int64
	QueryLeafCount   int64
	QueryBranchCount int64
}

func (m *TreeMetrics) Report() {
	fmt.Printf("Pool:\n gets: %s, returns: %s, faults: %s, evicts: %s, evict miss %s\n",
		humanize.Comma(m.PoolGet),
		humanize.Comma(m.PoolReturn),
		humanize.Comma(m.PoolFault),
		humanize.Comma(m.PoolEvict),
		humanize.Comma(m.PoolEvictMiss),
	)

	fmt.Printf("\nTree:\n update: %s, new node: %s, delete: %s\n",
		humanize.Comma(m.TreeUpdate),
		humanize.Comma(m.TreeNewNode),
		humanize.Comma(m.TreeDelete))
}

func (s *StructMetrics) QueryReport(bins int) error {
	if s.QueryCount == 0 {
		return nil
	}

	fmt.Printf("queries=%s q/s=%s dur/q=%s dur=%s leaf-q=%s branch-q=%s leaf-miss=%s\n",
		humanize.Comma(s.QueryCount),
		humanize.Comma(int64(float64(s.QueryCount)/s.QueryTime.Seconds())),
		time.Duration(int64(s.QueryTime)/s.QueryCount),
		s.QueryTime.Round(time.Millisecond),
		humanize.Comma(s.QueryLeafCount),
		humanize.Comma(s.QueryBranchCount),
		humanize.Comma(s.QueryLeafMiss),
	)

	if bins > 0 {
		var histData []float64
		for _, d := range s.QueryDurations {
			if d > 50*time.Microsecond {
				continue
			}
			histData = append(histData, float64(d))
		}
		hist := histogram.Hist(bins, histData)
		err := histogram.Fprintf(os.Stdout, hist, histogram.Linear(10), func(v float64) string {
			return time.Duration(v).String()
		})
		if err != nil {
			return err
		}
	}

	s.SetQueryZero()

	return nil
}

func (s *StructMetrics) SetQueryZero() {
	s.QueryDurations = nil
	s.QueryTime = 0
	s.QueryCount = 0
	s.QueryLeafMiss = 0
	s.QueryLeafCount = 0
	s.QueryBranchCount = 0
}

func (s *StructMetrics) Add(os *StructMetrics) {
	s.WriteDurations = append(s.WriteDurations, os.WriteDurations...)
	s.WriteTime += os.WriteTime
	s.WriteLeaves += os.WriteLeaves
	s.WriteBranch += os.WriteBranch

	s.QueryDurations = append(s.QueryDurations, os.QueryDurations...)
	s.QueryTime += os.QueryTime
	s.QueryCount += os.QueryCount
	s.QueryLeafMiss += os.QueryLeafMiss
	s.QueryLeafCount += os.QueryLeafCount
	s.QueryBranchCount += os.QueryBranchCount
}
