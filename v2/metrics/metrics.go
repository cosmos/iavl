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

func (m *DbMetrics) QueryReport(bins int) error {
	if m.QueryCount == 0 {
		return nil
	}

	fmt.Printf("queries=%s q/s=%s dur/q=%s dur=%s leaf-q=%s branch-q=%s leaf-miss=%s\n",
		humanize.Comma(m.QueryCount),
		humanize.Comma(int64(float64(m.QueryCount)/m.QueryTime.Seconds())),
		time.Duration(int64(m.QueryTime)/m.QueryCount),
		m.QueryTime.Round(time.Millisecond),
		humanize.Comma(m.QueryLeafCount),
		humanize.Comma(m.QueryBranchCount),
		humanize.Comma(m.QueryLeafMiss),
	)

	if bins > 0 {
		var histData []float64
		for _, d := range m.QueryDurations {
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

	m.SetQueryZero()

	return nil
}

func (m *DbMetrics) SetQueryZero() {
	m.QueryDurations = nil
	m.QueryTime = 0
	m.QueryCount = 0
	m.QueryLeafMiss = 0
	m.QueryLeafCount = 0
	m.QueryBranchCount = 0
}

func (m *DbMetrics) Add(o *DbMetrics) {
	m.WriteDurations = append(m.WriteDurations, o.WriteDurations...)
	m.WriteTime += o.WriteTime
	m.WriteLeaves += o.WriteLeaves

	m.QueryDurations = append(m.QueryDurations, o.QueryDurations...)
	m.QueryTime += o.QueryTime
	m.QueryCount += o.QueryCount
	m.QueryLeafMiss += o.QueryLeafMiss
	m.QueryLeafCount += o.QueryLeafCount
	m.QueryBranchCount += o.QueryBranchCount
}
