package main

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

//go:embed VERSION
var rawVersion string

var version string

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
	Name:    "archiver",
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
				ipfsFlags,
				[]cli.Flag{
					&cli.StringFlag{
						Name:  "output",
						Usage: "Path to write output files.",
						Value: "/data/filecoin/archiver/shipped", // TODO: remove default
					},
					&cli.Int64Flag{
						Name:  "min-height",
						Usage: "Minimum height that should be exported. This may be used for nodes that do not have full state history.",
						Value: 1005360, // TODO: remove default
					},
					&cli.StringFlag{
						Name:  "tasks",
						Usage: "Comma separated list of tasks that are allowed to be processed. Default is all tasks.",
						Value: "",
					},
				},
			),
			Action: func(cc *cli.Context) error {
				ctx := cc.Context
				tasks := cc.String("tasks")

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

				if err := verifyShipDependencies(); err != nil {
					return fmt.Errorf("unable to ship files: %w", err)
				}

				peer, err := NewPeer(&PeerConfig{
					ListenAddr:    ipfsConfig.listenAddr,
					DatastorePath: ipfsConfig.datastorePath,
					Libp2pKeyFile: ipfsConfig.libp2pKeyfile,
				})
				if err != nil {
					return fmt.Errorf("new ipfs peer: %w", err)
				}

				p := firstExportPeriodAfter(cc.Int64("min-height"), networkConfig.genesisTs)
				for {
					em, err := manifestForPeriod(ctx, p, networkConfig.name, networkConfig.genesisTs, cc.String("output"), storageConfig.schemaVersion, allowedTables, peer)
					if err != nil {
						return fmt.Errorf("failed to create manifest for %s: %w", p.Date.String(), err)
					}

					if err := processExport(ctx, em, cc.String("output"), peer); err != nil {
						return fmt.Errorf("failed to process export for %s: %w", p.Date.String(), err)
					}

					p = p.Next()
				}
			},
		},

		{
			Name:   "stat",
			Usage:  "Report the status of export files.",
			Before: configure,
			Flags: flagSet(
				loggingFlags,
				networkFlags,
				storageFlags,
				ipfsFlags,
				[]cli.Flag{
					&cli.StringFlag{
						Name:  "output",
						Usage: "Path to write output files.",
						Value: "/data/filecoin/archiver/shipped", // TODO: remove default
					},
					&cli.BoolFlag{
						Name:  "shipped",
						Usage: "Include files that have been shipped.",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  "announced",
						Usage: "Include files that have been announced on IPFS.",
						Value: false,
					},
					&cli.StringFlag{
						Name:  "from-date",
						Usage: "Include only files that are exported on or after this date.",
					},
					&cli.StringFlag{
						Name:  "to-date",
						Usage: "Include only files that are exported on or before this date.",
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

				peer, err := NewPeer(&PeerConfig{
					ListenAddr:    ipfsConfig.listenAddr,
					DatastorePath: ipfsConfig.datastorePath,
					Libp2pKeyFile: ipfsConfig.libp2pKeyfile,
				})
				if err != nil {
					return fmt.Errorf("new ipfs peer: %w", err)
				}

				includeShipped := cc.Bool("shipped")
				includeAnnounced := cc.Bool("announced")

				current := CurrentHeight(networkConfig.genesisTs)

				for p := firstExportPeriod(networkConfig.genesisTs); p.EndHeight+Finality < current; p = p.Next() {
					if !fromDate.IsZero() && fromDate.After(p.Date) {
						continue
					}

					if !toDate.IsZero() && p.Date.After(toDate) {
						continue
					}

					em, err := manifestForPeriod(ctx, p, networkConfig.name, networkConfig.genesisTs, cc.String("output"), storageConfig.schemaVersion, TableList, peer)
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

						announced := "x"
						if ef.Announced {
							if !includeAnnounced {
								continue
							}
							announced = "A"
							desc += "\t" + ef.Cid.String()
						}

						if includeAnnounced {
							fmt.Printf("%s%s %s\n", shipped, announced, desc)
						}
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
						Usage:    "Tables to verify, comma separated.",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "name",
						Usage: "Name of the export.",
						Value: "export-1005360-1008239",
					},
				},
			),
			Action: func(cc *cli.Context) error {
				tables := strings.Split(cc.String("tables"), ",")

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

				rep, err := verifyTasks(wi, tasklist)
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
				ipfsFlags,
				[]cli.Flag{
					&cli.StringFlag{
						Name:     "tables",
						Usage:    "Tables to ship, comma separated.",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "name",
						Usage: "Name of the export.",
						Value: "export-1005360-1008239",
					},
					&cli.StringFlag{
						Name:     "date",
						Usage:    "Date covered by the export.",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "output",
						Usage: "Path to write output files.",
						Value: "/data/filecoin/archiver/shipped", // TODO: remove default
					},
					&cli.StringFlag{
						Name:   "compression",
						Usage:  "Type of compression to use.",
						Value:  "gz",
						Hidden: true,
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

				if err := verifyShipDependencies(); err != nil {
					return fmt.Errorf("unable to ship files: %w", err)
				}

				wi := WalkInfo{
					Name:   cc.String("name"),
					Path:   storageConfig.path,
					Format: "csv",
				}

				tables := strings.Split(cc.String("tables"), ",")
				for _, table := range tables {
					if _, ok := TablesByName[table]; !ok {
						return fmt.Errorf("unknown table %q", table)
					}

					ef := ExportFile{
						Date:        dt,
						Schema:      storageConfig.schemaVersion,
						Network:     networkConfig.name,
						TableName:   table,
						Format:      "csv",
						Compression: c.Extension,
					}

					err := shipFile(cc.Context, &ef, wi, cc.String("output"))
					if err != nil {
						return fmt.Errorf("ship file: %w", err)
					}
				}
				return nil
			},
		},

		{
			Name:   "announce",
			Usage:  "Announce shipped files.",
			Before: configure,
			Flags: flagSet(
				loggingFlags,
				storageFlags,
				networkFlags,
				ipfsFlags,
				[]cli.Flag{
					&cli.StringFlag{
						Name:     "tables",
						Usage:    "Tables to ship, comma separated.",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "name",
						Usage: "Name of the export.",
						Value: "export-1005360-1008239", // TODO: remove default
					},
					&cli.StringFlag{
						Name:     "date",
						Usage:    "Date covered by the export.",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "output",
						Usage: "Path to write output files.",
						Value: "/data/filecoin/archiver/shipped", // TODO: remove default
					},
					&cli.StringFlag{
						Name:   "compression",
						Usage:  "Type of compression used.",
						Value:  "gz",
						Hidden: true,
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

				peer, err := NewPeer(&PeerConfig{
					ListenAddr:    ipfsConfig.listenAddr,
					DatastorePath: ipfsConfig.datastorePath,
					Libp2pKeyFile: ipfsConfig.libp2pKeyfile,
				})
				if err != nil {
					return fmt.Errorf("new ipfs peer: %w", err)
				}

				tables := strings.Split(cc.String("tables"), ",")
				for _, table := range tables {
					if _, ok := TablesByName[table]; !ok {
						return fmt.Errorf("unknown table %q", table)
					}

					ef := ExportFile{
						Date:        dt,
						Schema:      storageConfig.schemaVersion,
						Network:     networkConfig.name,
						TableName:   table,
						Format:      "csv",
						Compression: c.Extension,
					}

					_, err := peer.addFile(cc.Context, &ef, cc.String("output"))
					if err != nil {
						return fmt.Errorf("add file: %w", err)
					}
				}

				if err := peer.provide(cc.Context); err != nil {
					return fmt.Errorf("provide: %w", err)
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
