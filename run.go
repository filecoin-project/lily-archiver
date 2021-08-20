package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/filecoin-project/sentinel-visor/commands"
	"github.com/filecoin-project/sentinel-visor/lens/lily"
	"github.com/filecoin-project/sentinel-visor/schedule"
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

func processExport(ctx context.Context, em *ExportManifest) error {
	ll := logger.With("date", em.Period.Date.String(), "from", em.Period.StartHeight, "to", em.Period.EndHeight)

	if len(em.Files) == 0 {
		ll.Info("found all expected files, nothing to do")
		return nil
	}

	ll.Infof("export period is missing %d files", len(em.Files))

	// We must wait for one full finality after the end of the period before running the export
	earliestStartTs := HeightToUnix(em.Period.EndHeight+Finality, networkConfig.genesisTs)
	ll.Infof("earliest time this export can start: %d (%s)", earliestStartTs, time.Unix(earliestStartTs, 0))

	// TODO: wait until earliest export time

	// TODO: run tasks other than blocks+chaineconomics with an extra height
	// TODO: always run consensus task
	walkCfg, err := walkForManifest(em)
	if err != nil {
		return fmt.Errorf("build walk for yesterday: %w", err)
	}
	ll.Infof("using tasks %s", strings.Join(walkCfg.Tasks, ","))

	api, closer, err := commands.GetAPI(ctx, lilyConfig.apiAddr, lilyConfig.apiToken)
	if err != nil {
		return fmt.Errorf("could not access lily api: %w", err)
	}
	defer closer()

	// TODO: don't start the walk if we have raw exports and they have no execution errors

	jobID, err := api.LilyWalk(ctx, walkCfg)
	if err != nil {
		return fmt.Errorf("failed to create walk: %w", err)
	}

	ll.Infof("started walk with id %d", jobID)

	if err := WaitUntil(ctx, jobHasEnded(api, jobID), time.Minute*5); err != nil {
		return fmt.Errorf("failed waiting for job to finish: %w", err)
	}

	jr, err := getJobResult(ctx, api, jobID)
	if err != nil {
		return fmt.Errorf("failed reading job result: %w", err)
	}

	if jr.Error != "" {
		// TODO: retry
		return fmt.Errorf("job failed with error: %s", jr.Error)
	}

	ll.Info("export complete")

	// TODO: use processing report to look for execution errors

	// TODO: compress and copy file to correct location

	return nil
}

func jobHasEnded(api lily.LilyAPI, id schedule.JobID) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		jr, err := getJobResult(ctx, api, id)
		if err != nil {
			return false, err
		}

		if jr.Running {
			return false, nil
		}
		return true, nil
	}
}

func getJobResult(ctx context.Context, api lily.LilyAPI, id schedule.JobID) (*schedule.JobResult, error) {
	jobs, err := api.LilyJobList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}

	for _, jr := range jobs {
		if jr.ID == id {
			return &jr, nil
		}
	}

	return nil, fmt.Errorf("job %d not found", id)
}
