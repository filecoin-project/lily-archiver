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

func main() {
	ctx := context.TODO()

	version := rawVersion
	if idx := strings.Index(version, "\n"); idx > -1 {
		version = version[:idx]
	}

	app := &cli.App{
		Name:    "archiver",
		Usage:   "produces regular archives of on-chain state for the Filecoin network.",
		Version: version,
		Commands: []*cli.Command{
			RunCmd,
			ScanCmd,
			VerifyCmd,
			ShipCmd,
		},
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
}

func flagSet(fs ...[]cli.Flag) []cli.Flag {
	var flags []cli.Flag

	for _, f := range fs {
		flags = append(flags, f...)
	}

	return flags
}
