package bench

import (
	"log/slog"
	"net/http"
	"os"
	"runtime/pprof"
	"time"

	"github.com/cosmos/iavl/v2"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
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
		cpuProfile    string
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
			if cpuProfile != "" {
				f, err := os.Create(cpuProfile)
				if err != nil {
					return err
				}
				if err := pprof.StartCPUProfile(f); err != nil {
					return err
				}
				defer func() {
					pprof.StopCPUProfile()
					f.Close()
				}()
			}
			treeOpts := iavl.DefaultTreeOptions()
			treeOpts.CheckpointInterval = 80
			treeOpts.StateStorage = true
			treeOpts.HeightFilter = 1
			treeOpts.EvictionDepth = 22
			treeOpts.MetricsProxy = metrics.NewStructMetrics()
			if usePrometheus {
				treeOpts.MetricsProxy = newPrometheusMetricsProxy()
			}

			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
			var multiTree *iavl.MultiTree
			if loadSnapshot {
				var err error
				multiTree, err = iavl.ImportMultiTree(logger, 1, dbPath, treeOpts)
				if err != nil {
					return err
				}
			} else {
				multiTree = iavl.NewMultiTree(logger, dbPath, treeOpts)
				if err := multiTree.MountTrees(); err != nil {
					return err
				}
				if err := multiTree.LoadVersion(1); err != nil {
					return err
				}
				if err := multiTree.WarmLeaves(); err != nil {
					return err
				}
			}

			opts := testutil.CompactedChangelogs(changelogPath)
			opts.SampleRate = 250_000

			// opts.Until = 1_000
			// opts.UntilHash = "557663181d9ab97882ecfc6538e3b4cfe31cd805222fae905c4b4f4403ca5cda"
			opts.Until = 500
			opts.UntilHash = "2670bd5767e70f2bf9e4f723b5f205759e39afdb5d8cfb6b54a4a3ecc27a1377"

			_, err := multiTree.TestBuild(opts)
			return err
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "/tmp/iavl-v2", "the path to the database at version 1")
	cmd.Flags().StringVar(&changelogPath, "changelog", "/tmp/osmo-like-many/v2", "the path to the changelog")
	cmd.Flags().BoolVar(&loadSnapshot, "snapshot", false, "load the snapshot at version 1 before running the benchmarks (loads full tree into memory)")
	cmd.Flags().BoolVar(&usePrometheus, "prometheus", false, "enable prometheus metrics")
	cmd.Flags().StringVar(&cpuProfile, "cpu-profile", "", "write cpu profile to file")

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
