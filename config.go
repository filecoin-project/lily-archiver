package main

import (
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var (
	logger = logging.Logger("archiver")

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
		genesisTs int64
		name      string
	}

	networkFlags = []cli.Flag{
		&cli.StringFlag{
			Name:        "network",
			Usage:       "Name of the filecoin network.",
			Value:       "mainnet",
			Hidden:      true,
			Destination: &networkConfig.name,
		},
		&cli.Int64Flag{
			Name:        "genesis-ts",
			Usage:       "Unix timestamp of the genesis epoch. Defaults to mainnet genesis.",
			Value:       MainnetGenesisTs, // could be overridden for test nets
			Hidden:      true,
			Destination: &networkConfig.genesisTs,
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
			Usage:       "Multiaddress of the lily API.",
			Value:       "/ip4/127.0.0.1/tcp/1234",
			Destination: &lilyConfig.apiAddr,
		},
		&cli.StringFlag{
			Name:        "lily-token",
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
			Usage:       "Path in which files written by lily will be found. This should be same as the path for the configured storage in visor config.",
			Value:       "/data/filecoin/archiver/rawcsv/calibnet", // TODO: remove default
			Destination: &storageConfig.path,
		},
		&cli.StringFlag{
			Name:        "storage-name",
			Usage:       "Name of the lily storage profile to use when executing a walk.",
			Value:       "CSV", // TODO: remove default
			Destination: &storageConfig.name,
		},
		&cli.IntFlag{
			Name:        "storage-schema",
			Usage:       "Version of schema to used by storage.",
			Value:       1,
			Hidden:      true,
			Destination: &storageConfig.schemaVersion,
		},
	}
)

func configure(_ *cli.Context) error {
	if err := logging.SetLogLevel("archiver", loggingConfig.level); err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}
	return nil
}
