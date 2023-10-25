package metrics

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aybabtme/uniplot/histogram"
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

func (m *TreeMetrics) QueryReport(bins int) error {
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

	m.QueryDurations = nil
	m.QueryTime = 0
	m.QueryCount = 0
	m.QueryLeafMiss = 0
	m.QueryLeafCount = 0
	m.QueryBranchCount = 0

	return nil
}

type Counter interface {
	Inc()
}

type Gauge interface {
	Add(float64)
	Sub(float64)
	Set(float64)
}

var Default = NewMetrics()

type Collectable interface {
	Collect() MetricPoint
}

type Metrics struct {
	metrics []Collectable
	Series  map[string][]MetricPoint
}

func NewMetrics() *Metrics {
	return &Metrics{
		Series: make(map[string][]MetricPoint),
	}
}

type MetricPoint struct {
	time  int64
	value int64
	path  string
}

func (m *Metrics) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 2)
	flush := func() {
		t := time.Now().Unix()
		for _, c := range m.metrics {
			pt := c.Collect()
			pt.time = t
			m.Series[pt.path] = append(m.Series[pt.path], pt)
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return nil
		case <-ticker.C:
			flush()
		}
	}
}

func (m *Metrics) Print() string {
	builder := strings.Builder{}
	for path, series := range m.Series {
		for _, pt := range series {
			builder.WriteString(fmt.Sprintf("%s %s %d\n", path, humanize.Comma(pt.value), pt.time))
		}
	}
	return builder.String()
}

func (m *Metrics) NewCounter(path string) Counter {
	c := &counter{
		path: path,
	}
	m.metrics = append(m.metrics, c)
	return c
}

func (m *Metrics) NewGauge(path string) Gauge {
	g := &gauge{
		path: path,
	}
	m.metrics = append(Default.metrics, g)
	return g
}

type counter struct {
	path  string
	count int64
}

func (c *counter) Inc() {
	atomic.AddInt64(&c.count, 1)
}

func (c *counter) Collect() MetricPoint {
	return MetricPoint{
		value: c.count,
		path:  c.path,
	}
}

type gauge struct {
	metrics *Metrics
	path    string
	valBits uint64
}

func (g *gauge) Set(val float64) {
	if g == nil {
		return
	}
	atomic.StoreUint64(&g.valBits, math.Float64bits(val))
}

func (g *gauge) SetToCurrentTime() {
	g.Set(float64(time.Now().UnixNano()) / 1e9)
}

func (g *gauge) Inc() {
	g.Add(1)
}

func (g *gauge) Dec() {
	g.Add(-1)
}

func (g *gauge) Add(val float64) {
	for {
		oldBits := atomic.LoadUint64(&g.valBits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + val)
		if atomic.CompareAndSwapUint64(&g.valBits, oldBits, newBits) {
			return
		}
	}
}

func (g *gauge) Sub(val float64) {
	g.Add(val * -1)
}

func (g *gauge) Collect() MetricPoint {
	val := math.Float64frombits(atomic.LoadUint64(&g.valBits))
	return MetricPoint{
		value: int64(val),
		path:  g.path,
	}
}
