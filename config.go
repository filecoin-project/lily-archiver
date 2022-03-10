package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"contrib.go.opencensus.io/exporter/prometheus"
	logging "github.com/ipfs/go-log/v2"
	metrics "github.com/ipfs/go-metrics-interface"
	metricsprom "github.com/ipfs/go-metrics-prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
)

const (
	DefaultFilePerms = 0o664
	DefaultDirPerms  = 0o775
)

var (
	logger = logging.Logger(appName)

	loggingConfig struct {
		level string
	}

	loggingFlags = []cli.Flag{
		&cli.StringFlag{
			Name:        "log-level",
			EnvVars:     []string{"GOLOG_LOG_LEVEL"},
			Value:       "ERROR",
			Usage:       "Set the default log level for all loggers to `LEVEL`",
			Destination: &loggingConfig.level,
		},
	}
)

var (
	networkConfig struct {
		genesisTs       int64
		name            string
		upgradeSchedule string
	}

	networkFlags = []cli.Flag{
		&cli.StringFlag{
			Name:        "network",
			EnvVars:     []string{"ARCHIVER_NETWORK"},
			Usage:       "Name of the filecoin network.",
			Value:       "mainnet",
			Hidden:      true,
			Destination: &networkConfig.name,
		},
		&cli.Int64Flag{
			Name:        "genesis-ts",
			EnvVars:     []string{"ARCHIVER_GENESIS_TS"},
			Usage:       "Unix timestamp of the genesis epoch. Defaults to mainnet genesis.",
			Value:       MainnetGenesisTs, // could be overridden for test nets
			Hidden:      true,
			Destination: &networkConfig.genesisTs,
		},
		&cli.StringFlag{
			Name:        "upgrade-schedule",
			EnvVars:     []string{"ARCHIVER_UPGRADE_SCHEDULE"},
			Usage:       "Network version upgrade schedule for the network. Use a comma separated list of version:height entries. Only upgrades that affect Lily model versions need be included.",
			Value:       "14:1231620", // mainnet by default
			Hidden:      true,
			Destination: &networkConfig.upgradeSchedule,
		},
	}
)

var (
	lilyConfig struct {
		apiAddr  string
		apiToken string
	}

	lilyFlags = []cli.Flag{
		&cli.StringFlag{
			Name:        "lily-addr",
			EnvVars:     []string{"ARCHIVER_LILY_ADDR"},
			Usage:       "Multiaddress of the lily API.",
			Value:       "/ip4/127.0.0.1/tcp/1234",
			Destination: &lilyConfig.apiAddr,
		},
		&cli.StringFlag{
			Name:        "lily-token",
			EnvVars:     []string{"ARCHIVER_LILY_TOKEN"},
			Usage:       "Authentication token for lily API.",
			Destination: &lilyConfig.apiToken,
		},
	}
)

var (
	storageConfig struct {
		name          string // name of storage configured in lily
		path          string // path that storage will write to
		schemaVersion int    // version of the lily schema used by the storage
	}

	storageFlags = []cli.Flag{
		&cli.StringFlag{
			Name:        "storage-path",
			EnvVars:     []string{"ARCHIVER_STORAGE_PATH"},
			Usage:       "Path in which files written by lily will be found. This should be same as the path for the configured storage in visor config.",
			Value:       "/data/filecoin/archiver/rawcsv/calibnet", // TODO: remove default
			Destination: &storageConfig.path,
		},
		&cli.StringFlag{
			Name:        "storage-name",
			EnvVars:     []string{"ARCHIVER_STORAGE_NAME"},
			Usage:       "Name of the lily storage profile to use when executing a walk.",
			Value:       "CSV", // TODO: remove default
			Destination: &storageConfig.name,
		},
		&cli.IntFlag{
			Name:        "storage-schema",
			EnvVars:     []string{"ARCHIVER_STORAGE_SCHEMA"},
			Usage:       "Version of schema to used by storage.",
			Value:       1,
			Hidden:      true,
			Destination: &storageConfig.schemaVersion,
		},
	}
)

var (
	diagnosticsConfig struct {
		debugAddr      string
		prometheusAddr string
	}

	diagnosticsFlags = []cli.Flag{
		&cli.StringFlag{
			Name:        "debug-addr",
			Usage:       "Network address to start a debug http server on (example: 127.0.0.1:8080)",
			Value:       "",
			Destination: &diagnosticsConfig.debugAddr,
		},
		&cli.StringFlag{
			Name:        "prometheus-addr",
			EnvVars:     []string{"ARCHIVER_PROMETHEUS_ADDR"},
			Usage:       "Network address to start a prometheus metric exporter server on (example: :9991)",
			Value:       "",
			Destination: &diagnosticsConfig.prometheusAddr,
		},
	}
)

func configure(_ *cli.Context) error {
	if err := logging.SetLogLevel(appName, loggingConfig.level); err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}

	if err := setUpgradeSchedule(networkConfig.upgradeSchedule); err != nil {
		return fmt.Errorf("invalid upgrade schedule: %w", err)
	}

	if diagnosticsConfig.debugAddr != "" {
		if err := startDebugServer(); err != nil {
			return fmt.Errorf("start debug server: %w", err)
		}
	}

	if diagnosticsConfig.prometheusAddr != "" {
		if err := startPrometheusServer(); err != nil {
			return fmt.Errorf("start prometheus server: %w", err)
		}
	}

	return nil
}

func startDebugServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	go func() {
		if err := http.ListenAndServe(diagnosticsConfig.debugAddr, mux); err != nil {
			logger.Errorw("debug server failed", "error", err)
		}
	}()
	return nil
}

func startPrometheusServer() error {
	// Bind the ipfs metrics interface to prometheus
	if err := metricsprom.Inject(); err != nil {
		logger.Errorw("unable to inject prometheus ipfs/go-metrics exporter; some metrics will be unavailable", "error", err)
	}

	pe, err := prometheus.NewExporter(prometheus.Options{
		Namespace:  appName,
		Registerer: prom.DefaultRegisterer,
		Gatherer:   prom.DefaultGatherer,
	})
	if err != nil {
		return fmt.Errorf("new prometheus exporter: %w", err)
	}

	// register prometheus with opencensus
	view.RegisterExporter(pe)
	view.SetReportingPeriod(2 * time.Second)

	mux := http.NewServeMux()
	mux.Handle("/metrics", pe)
	go func() {
		if err := http.ListenAndServe(diagnosticsConfig.prometheusAddr, mux); err != nil {
			logger.Errorw("prometheus server failed", "error", err)
		}
	}()
	return nil
}

// metrics
var (
	exportLastCompletedHeightGauge metrics.Gauge
	processExportStartedCounter    metrics.Counter
	processExportErrorsCounter     metrics.Counter
	lilyConnectionErrorsCounter    metrics.Counter
	lilyJobErrorsCounter           metrics.Counter
	walkErrorsCounter              metrics.Counter
	verifyTableErrorsCounter       metrics.Counter
	shipTableErrorsCounter         metrics.Counter
)

func setupMetrics(ctx context.Context) {
	exportLastCompletedHeightGauge = metrics.NewCtx(ctx, "export_last_completed_height", "Height of last completed export").Gauge()
	lilyConnectionErrorsCounter = metrics.NewCtx(ctx, "lily_connection_errors_total", "Total number of errors encountered connecting to lily node").Counter()
	lilyJobErrorsCounter = metrics.NewCtx(ctx, "lily_job_errors_total", "Total number of errors encountered while managing lily jobs").Counter()
	processExportStartedCounter = metrics.NewCtx(ctx, "process_export_started_total", "Total number of exports that have started processing").Counter()
	processExportErrorsCounter = metrics.NewCtx(ctx, "process_export_errors_total", "Total number of errors encountered processing an export").Counter()
	walkErrorsCounter = metrics.NewCtx(ctx, "walk_errors_total", "Total number of errors encountered creating and waiting for walks to complete").Counter()
	verifyTableErrorsCounter = metrics.NewCtx(ctx, "verify_table_errors_total", "Total number of errors encountered verifying an exported table").Counter()
	shipTableErrorsCounter = metrics.NewCtx(ctx, "ship_table_errors_total", "Total number of errors encountered shipping an exported table").Counter()
}
