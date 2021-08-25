package main

import (
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"
)

var runConfig struct {
	outputPath string
	minHeight  int64
}

var RunCmd = &cli.Command{
	Name:   "run",
	Usage:  "Produce daily archives of data.",
	Before: configure,
	Flags: flagSet(
		loggingFlags,
		networkFlags,
		lilyFlags,
		storageFlags,
		[]cli.Flag{
			&cli.StringFlag{
				Name:        "output",
				Usage:       "Path to write output files.",
				Value:       "/mnt/disk1/data/export", // TODO: remove default
				Destination: &runConfig.outputPath,
			},
			&cli.Int64Flag{
				Name:        "min-height",
				Usage:       "Minimum height that should be exported. This may be used for nodes that do not have full state history.",
				Value:       1005360, // TODO: remove default
				Destination: &runConfig.minHeight,
			},
		},
	),
	Action: func(cc *cli.Context) error {
		ctx := cc.Context

		p := firstExportPeriodAfter(runConfig.minHeight, networkConfig.genesisTs)

		for {
			em, err := manifestForPeriod(p, networkConfig.name, networkConfig.genesisTs, runConfig.outputPath, storageConfig.schemaVersion)
			if err != nil {
				return fmt.Errorf("failed to create manifest for %s: %w", p.Date.String(), err)
			}

			if err := processExport(ctx, em); err != nil {
				return fmt.Errorf("failed to process export for %s: %w", p.Date.String(), err)
			}

			p = p.Next()

		}
	},
}

var scanConfig struct {
	outputPath string
}

var ScanCmd = &cli.Command{
	Name:   "scan",
	Usage:  "Scan the output directory hierarchy and build a manifest of files that should be present.",
	Before: configure,
	Flags: flagSet(
		loggingFlags,
		networkFlags,
		storageFlags,
		[]cli.Flag{
			&cli.StringFlag{
				Name:        "output",
				Usage:       "Path to write output files.",
				Value:       "/mnt/disk1/data/export", // TODO: remove default
				Destination: &scanConfig.outputPath,
			},
		},
	),
	Action: func(cc *cli.Context) error {
		current := CurrentHeight(networkConfig.genesisTs)
		logger.Infof("current epoch: %d\n", current)

		p := firstExportPeriod(networkConfig.genesisTs)
		for p.EndHeight+Finality < current {
			logger.Infof("export for %s uses range %d-%d", p.Date.String(), p.StartHeight, p.EndHeight)

			em, err := manifestForPeriod(p, networkConfig.name, networkConfig.genesisTs, scanConfig.outputPath, storageConfig.schemaVersion)
			if err != nil {
				return fmt.Errorf("build manifest for period: %w", err)
			}

			for _, ef := range em.Files {
				logger.Infof("export for %s is missing file %s", p.Date.String(), ef.Path())
			}

			p = p.Next()
		}

		return nil
	},
}

var verifyConfig struct {
	tables string
	name   string
}

var VerifyCmd = &cli.Command{
	Name:   "verify",
	Usage:  "Verify raw export files.",
	Before: configure,
	Flags: flagSet(
		loggingFlags,
		networkFlags,
		storageFlags,
		[]cli.Flag{
			&cli.StringFlag{
				Name:        "tables",
				Usage:       "Tables to verify, comma separated.",
				Required:    true,
				Destination: &verifyConfig.tables,
			},
			&cli.StringFlag{
				Name:        "name",
				Usage:       "Name of the export.",
				Value:       "export-1005360-1008239",
				Destination: &verifyConfig.name,
			},
		},
	),
	Action: func(cc *cli.Context) error {
		tables := strings.Split(verifyConfig.tables, ",")

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

		rep, err := verifyTasks(storageConfig.path, verifyConfig.name, tasklist)
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
				fmt.Printf("%s: verified ok\n", table)
				continue
			}
			rs := ranges(status.Missing)
			for _, r := range rs {
				fmt.Printf("%s: found gap from %d to %d", table, r.Lower, r.Upper)
				reportFailed = true
			}

			if len(status.Error) > 0 {
				fmt.Printf("%s: found %d errors", table, len(status.Error))
				reportFailed = true
			}
			if len(status.Unexpected) > 0 {
				fmt.Printf("%s: found %d unexpected processing reports", table, len(status.Unexpected))
				reportFailed = true
			}

		}

		if reportFailed {
			return fmt.Errorf("one or more verification failures found")
		}
		return nil
	},
}

var shipConfig struct {
	tables      string
	compression string
	name        string
	outputPath  string
}

var ShipCmd = &cli.Command{
	Name:   "ship",
	Usage:  "Ship raw export files.",
	Before: configure,
	Flags: flagSet(
		loggingFlags,
		storageFlags,
		[]cli.Flag{
			&cli.StringFlag{
				Name:        "tables",
				Usage:       "Tables to ship, comma separated.",
				Required:    true,
				Destination: &shipConfig.tables,
			},
			&cli.StringFlag{
				Name:        "name",
				Usage:       "Name of the export.",
				Value:       "export-1005360-1008239",
				Destination: &shipConfig.name,
			},
			&cli.StringFlag{
				Name:        "output",
				Usage:       "Path to write output files.",
				Value:       "/mnt/disk1/data/export", // TODO: remove default
				Destination: &shipConfig.outputPath,
			},
			&cli.StringFlag{
				Name:        "compression",
				Usage:       "Type of compression to use.",
				Value:       "gz",
				Hidden:      true,
				Destination: &shipConfig.compression,
			},
		},
	),
	Action: func(cc *cli.Context) error {
		c, ok := CompressionByName[shipConfig.compression]
		if !ok {
			return fmt.Errorf("unknown compression %q", shipConfig.compression)
		}

		tables := strings.Split(shipConfig.tables, ",")
		for _, table := range tables {
			if _, ok := TablesByName[table]; !ok {
				return fmt.Errorf("unknown table %q", table)
			}
			err := shipTable(table, shipConfig.outputPath, storageConfig.path, shipConfig.name, c)
			if err != nil {
				return fmt.Errorf("ship table: %w", err)
			}
		}
		return nil
	},
}
