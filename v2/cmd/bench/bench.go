package bench

import (
	"net/http"
	"testing"
	"time"

	"github.com/cosmos/iavl/v2"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "run benchmarks",
	}
	cmd.AddCommand(benchCommand())
	return cmd
}

func benchCommand() *cobra.Command {
	var (
		dbPath        string
		changelogPath string
		loadSnapshot  bool
		usePrometheus bool
	)
	cmd := &cobra.Command{
		Use:   "std",
		Short: "run the std development benchmark",
		Long: `Runs a longer benchmark for the IAVL tree. This is useful for development and testing.
Pre-requisites this command:
$ go run ./cmd gen tree --db /tmp/iavl-v2 --limit 1 --type osmo-like-many
mkdir -p /tmp/osmo-like-many/v2 && go run ./cmd gen emit --start 2 --limit 1000 --type osmo-like-many --out /tmp/osmo-like-many/v2

Optional for --snapshot arg:
$ go run ./cmd snapshot --db /tmp/iavl-v2 --version 1
`,

		RunE: func(_ *cobra.Command, _ []string) error {
			t := &testing.T{}
			treeOpts := iavl.DefaultTreeOptions()
			treeOpts.CheckpointInterval = 80
			treeOpts.StateStorage = true
			treeOpts.HeightFilter = 1
			treeOpts.EvictionDepth = 22
			treeOpts.MetricsProxy = metrics.NewStructMetrics()
			if usePrometheus {
				treeOpts.MetricsProxy = newPrometheusMetricsProxy()
			}

			var multiTree *iavl.MultiTree
			if loadSnapshot {
				pool := iavl.NewNodePool()
				var err error
				multiTree, err = iavl.ImportMultiTree(pool, 1, dbPath, treeOpts)
				require.NoError(t, err)
			} else {
				multiTree = iavl.NewMultiTree(dbPath, treeOpts)
				require.NoError(t, multiTree.MountTrees())
				require.NoError(t, multiTree.LoadVersion(1))
				require.NoError(t, multiTree.WarmLeaves())
			}

			opts := testutil.CompactedChangelogs(changelogPath)
			opts.SampleRate = 250_000

			// opts.Until = 1_000
			// opts.UntilHash = "557663181d9ab97882ecfc6538e3b4cfe31cd805222fae905c4b4f4403ca5cda"
			opts.Until = 500
			opts.UntilHash = "2670bd5767e70f2bf9e4f723b5f205759e39afdb5d8cfb6b54a4a3ecc27a1377"

			multiTree.TestBuild(t, opts)
			return nil
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "/tmp/iavl-v2", "the path to the database at version 1")
	cmd.Flags().StringVar(&changelogPath, "changelog", "/tmp/osmo-like-many/v2", "the path to the changelog")
	cmd.Flags().BoolVar(&loadSnapshot, "snapshot", false, "load the snapshot at version 1 before running the benchmarks (loads full tree into memory)")
	cmd.Flags().BoolVar(&usePrometheus, "prometheus", false, "enable prometheus metrics")

	if err := cmd.MarkFlagRequired("changelog"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("db"); err != nil {
		panic(err)
	}
	return cmd
}

var _ metrics.Proxy = &prometheusMetricsProxy{}

type prometheusMetricsProxy struct {
	workingSize  prometheus.Gauge
	workingBytes prometheus.Gauge
}

func newPrometheusMetricsProxy() *prometheusMetricsProxy {
	p := &prometheusMetricsProxy{}
	p.workingSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "iavl_working_size",
		Help: "working size",
	})
	p.workingBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "iavl_working_bytes",
		Help: "working bytes",
	})
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(":2112", nil)
		if err != nil {
			panic(err)
		}
	}()
	return p
}

func (p *prometheusMetricsProxy) IncrCounter(_ float32, _ ...string) {
}

func (p *prometheusMetricsProxy) SetGauge(val float32, keys ...string) {
	k := keys[1]
	switch k {
	case "working_size":
		p.workingSize.Set(float64(val))
	case "working_bytes":
		p.workingBytes.Set(float64(val))
	}
}

func (p *prometheusMetricsProxy) MeasureSince(_ time.Time, _ ...string) {}
