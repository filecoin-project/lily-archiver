package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"time"

	metrics "github.com/ipfs/go-metrics-interface"
	"github.com/urfave/cli/v2"
)

//go:embed VERSION
var rawVersion string

var version string

const appName = "archiver"

func init() {
	version = rawVersion
	if idx := strings.Index(version, "\n"); idx > -1 {
		version = version[:idx]
	}
}

func main() {
	ctx := context.Background()
	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

var app = &cli.App{
	Name:    appName,
	Usage:   "produces regular archives of on-chain state for the Filecoin network.",
	Version: version,
	Commands: []*cli.Command{
		{
			Name:   "run",
			Usage:  "Produce daily archives of data.",
			Before: configure,
			Flags: flagSet(
				loggingFlags,
				networkFlags,
				lilyFlags,
				storageFlags,
				diagnosticsFlags,
				[]cli.Flag{
					&cli.StringFlag{
						Name:     "ship-path",
						EnvVars:  []string{"ARCHIVER_SHIP_PATH"},
						Usage:    "Path used to write verified exports from lily.",
						Required: true,
					},
					&cli.Int64Flag{
						Name:    "min-height",
						EnvVars: []string{"ARCHIVER_MIN_HEIGHT"},
						Usage:   "Minimum height that should be exported. This may be used for nodes that do not have full state history.",
						Value:   1005360, // TODO: remove default
					},
					&cli.StringFlag{
						Name:    "tasks",
						EnvVars: []string{"ARCHIVER_TASKS"},
						Usage:   "Comma separated list of tasks that are allowed to be processed. Default is all tasks.",
						Value:   "",
					},
					&cli.StringFlag{
						Name:    "compression",
						EnvVars: []string{"ARCHIVER_COMPRESSION"},
						Usage:   "Type of compression to use.",
						Value:   "gz",
						Hidden:  true,
					},
				},
			),
			Action: func(cc *cli.Context) error {
				ctx := metrics.CtxScope(cc.Context, appName)
				setupMetrics(ctx)

				tasks := cc.String("tasks")
				shipPath := cc.String("ship-path")
				minHeight := cc.Int64("min-height")

				// Build list of allowed tables. Could be all tables.
				var allowedTables []Table
				if tasks == "" || tasks == "all" {
					allowedTables = append(allowedTables, TableList...)
				} else {
					taskList := strings.Split(cc.String("tasks"), ",")
					if len(taskList) == 0 {
						return fmt.Errorf("invalid tasks specified")
					}
					for _, task := range taskList {
						tables, ok := TablesByTask[task]
						if !ok {
							return fmt.Errorf("unknown task: %s", task)
						}
						allowedTables = append(allowedTables, tables...)
					}
				}

				c, ok := CompressionByName[cc.String("compression")]
				if !ok {
					return fmt.Errorf("unknown compression %q", cc.String("compression"))
				}

				if err := verifyShipDependencies(shipPath, c); err != nil {
					return fmt.Errorf("unable to ship files: %w", err)
				}

				p := firstExportPeriodAfter(minHeight, networkConfig.genesisTs)
				for {
					// Retry this export until it works
					if err := WaitUntil(ctx, exportIsProcessed(p, allowedTables, c, shipPath), 0, time.Minute*15); err != nil {
						return fmt.Errorf("fatal error processing export: %w", err)
					}
					exportLastCompletedHeightGauge.Set(float64(p.EndHeight))
					p = p.Next()
				}
			},
		},

		{
			Name:   "stat",
			Usage:  "Report the status of exports.",
			Before: configure,
			Flags: flagSet(
				loggingFlags,
				networkFlags,
				storageFlags,
				[]cli.Flag{
					&cli.StringFlag{
						Name:     "ship-path",
						EnvVars:  []string{"ARCHIVER_SHIP_PATH"},
						Usage:    "Path used to write verified exports from lily.",
						Required: true,
					},
					&cli.BoolFlag{
						Name:    "shipped",
						EnvVars: []string{"ARCHIVER_SHIPPED"},
						Usage:   "Include files that have been shipped.",
						Value:   false,
					},
					&cli.StringFlag{
						Name:    "from-date",
						EnvVars: []string{"ARCHIVER_FROM_DATE"},
						Usage:   "Include only files that are exported on or after this date.",
					},
					&cli.StringFlag{
						Name:    "to-date",
						EnvVars: []string{"ARCHIVER_TO_DATE"},
						Usage:   "Include only files that are exported on or before this date.",
					},
					&cli.StringFlag{
						Name:    "compression",
						EnvVars: []string{"ARCHIVER_COMPRESSION"},
						Usage:   "Type of compression to use.",
						Value:   "gz",
						Hidden:  true,
					},
				},
			),
			Action: func(cc *cli.Context) error {
				ctx := cc.Context
				var fromDate Date
				if cc.IsSet("from-date") {
					var err error
					fromDate, err = DateFromString(cc.String("from-date"))
					if err != nil {
						return fmt.Errorf("invalid from date: %w", err)
					}
				}

				var toDate Date
				if cc.IsSet("to-date") {
					var err error
					toDate, err = DateFromString(cc.String("to-date"))
					if err != nil {
						return fmt.Errorf("invalid to date: %w", err)
					}
				}

				c, ok := CompressionByName[cc.String("compression")]
				if !ok {
					return fmt.Errorf("unknown compression %q", cc.String("compression"))
				}

				shipPath := cc.String("ship-path")
				includeShipped := cc.Bool("shipped")

				current := CurrentHeight(networkConfig.genesisTs)

				for p := firstExportPeriod(networkConfig.genesisTs); p.EndHeight+Finality < current; p = p.Next() {
					if !fromDate.IsZero() && fromDate.After(p.Date) {
						continue
					}

					if !toDate.IsZero() && p.Date.After(toDate) {
						continue
					}

					em, err := manifestForPeriod(ctx, p, networkConfig.name, networkConfig.genesisTs, shipPath, storageConfig.schemaVersion, TableList, c)
					if err != nil {
						return fmt.Errorf("build manifest for period: %w", err)
					}

					fmt.Printf("== %s (%d-%d)\n", p.Date.String(), p.StartHeight, p.EndHeight)
					for _, ef := range em.Files {
						desc := ef.TableName
						shipped := "x"
						if ef.Shipped {
							if !includeShipped {
								continue
							}
							shipped = "S"
							desc += "\t" + ef.Path()
						}

						fmt.Printf("%s %s\n", shipped, desc)
					}

				}

				return nil
			},
		},

		{
			Name:   "verify",
			Usage:  "Verify raw export files.",
			Before: configure,
			Flags: flagSet(
				loggingFlags,
				networkFlags,
				storageFlags,
				[]cli.Flag{
					&cli.StringFlag{
						Name:     "tables",
						EnvVars:  []string{"ARCHIVER_TABLES"},
						Usage:    "Tables to verify, comma separated.",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "name",
						EnvVars:  []string{"ARCHIVER_EXPORT_NAME"},
						Usage:    "Name of the export to be verified.",
						Required: true,
					},
				},
			),
			Action: func(cc *cli.Context) error {
				tables, err := parseTableList(cc.String("tables"))
				if err != nil {
					return fmt.Errorf("invalid tables: %w", err)
				}

				tasks := make(map[string]struct{}, 0)
				for _, table := range tables {
					t, ok := TablesByName[table]
					if !ok {
						return fmt.Errorf("unknown table %q", table)
					}
					tasks[t.Task] = struct{}{}
				}

				tasklist := make([]string, 0, len(tasks))
				for task := range tasks {
					tasklist = append(tasklist, task)
				}

				wi := WalkInfo{
					Name:   cc.String("name"),
					Path:   storageConfig.path,
					Format: "csv",
				}

				rep, err := verifyTasks(cc.Context, wi, tasklist)
				if err != nil {
					return fmt.Errorf("verify task: %w", err)
				}

				reportFailed := false
				for _, table := range tables {
					t, _ := TablesByName[table]
					status, ok := rep.TaskStatus[t.Task]
					if !ok {
						fmt.Printf("%s: verification failed, no further information\n", table)
						reportFailed = true
						continue
					}

					if len(status.Missing) == 0 && len(status.Error) == 0 && len(status.Unexpected) == 0 {
						fmt.Printf("%s: ok\n", table)
						continue
					}
					rs := ranges(status.Missing)
					for _, r := range rs {
						fmt.Printf("%s: found gap from %d to %d\n", table, r.Lower, r.Upper)
						reportFailed = true
					}

					if len(status.Error) > 0 {
						fmt.Printf("%s: found %d errors\n", table, len(status.Error))
						reportFailed = true
					}
					if len(status.Unexpected) > 0 {
						fmt.Printf("%s: found %d unexpected processing reports\n", table, len(status.Unexpected))
						reportFailed = true
					}

				}

				if reportFailed {
					return fmt.Errorf("one or more verification failures found")
				}
				return nil
			},
		},

		{
			Name:   "ship",
			Usage:  "Ship raw export files.",
			Before: configure,
			Flags: flagSet(
				loggingFlags,
				storageFlags,
				networkFlags,
				[]cli.Flag{
					&cli.StringFlag{
						Name:     "tables",
						EnvVars:  []string{"ARCHIVER_TABLES"},
						Required: true,
					},
					&cli.StringFlag{
						Name:     "name",
						EnvVars:  []string{"ARCHIVER_EXPORT_NAME"},
						Usage:    "Name of the export.",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "date",
						EnvVars:  []string{"ARCHIVER_EXPORT_DATE"},
						Usage:    "Date covered by the export.",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "ship-path",
						EnvVars:  []string{"ARCHIVER_SHIP_PATH"},
						Usage:    "Path used to write verified exports from lily.",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "compression",
						EnvVars: []string{"ARCHIVER_COMPRESSION"},
						Usage:   "Type of compression to use.",
						Value:   "gz",
						Hidden:  true,
					},
				},
			),
			Action: func(cc *cli.Context) error {
				c, ok := CompressionByName[cc.String("compression")]
				if !ok {
					return fmt.Errorf("unknown compression %q", cc.String("compression"))
				}

				dt, err := DateFromString(cc.String("date"))
				if err != nil {
					return fmt.Errorf("invalid date: %w", err)
				}

				shipPath := cc.String("ship-path")

				if err := verifyShipDependencies(shipPath, c); err != nil {
					return fmt.Errorf("unable to ship files: %w", err)
				}

				tables, err := parseTableList(cc.String("tables"))
				if err != nil {
					return fmt.Errorf("invalid tables: %w", err)
				}

				wi := WalkInfo{
					Name:   cc.String("name"),
					Path:   storageConfig.path,
					Format: "csv",
				}

				for _, table := range tables {
					ef := ExportFile{
						Date:        dt,
						Schema:      storageConfig.schemaVersion,
						Network:     networkConfig.name,
						TableName:   table,
						Format:      "csv",
						Compression: c,
					}

					err := shipExportFile(cc.Context, &ef, wi, shipPath)
					if err != nil {
						return fmt.Errorf("ship file: %w", err)
					}
				}
				return nil
			},
		},
	},
}

func flagSet(fs ...[]cli.Flag) []cli.Flag {
	var flags []cli.Flag

	for _, f := range fs {
		flags = append(flags, f...)
	}

	return flags
}

func parseTableList(str string) ([]string, error) {
	tables := strings.Split(str, ",")
	for _, table := range tables {
		if _, ok := TablesByName[table]; !ok {
			return nil, fmt.Errorf("unknown table %q", table)
		}
	}
	return tables, nil
}
