package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

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
