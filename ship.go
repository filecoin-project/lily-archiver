package main

import (
	"fmt"
	"os"
)

type Compression struct {
	Names     []string
	Extension string
}

var CompressionList = []Compression{
	{
		Names:     []string{"gzip", "gz"},
		Extension: "gz",
	},
}

// CompressionByName maps a compression name to the compression scheme.
var CompressionByName = map[string]Compression{}

func init() {
	for _, c := range CompressionList {
		for _, name := range c.Names {
			CompressionByName[name] = c
		}
	}
}

func shipTable(table string, outputPath string, exportPath string, prefix string, compression Compression) error {
	ll := logger.With("table", table)
	ll.Info("shipping table")

	fname := exportFilePath(exportPath, prefix, table)

	info, err := os.Stat(fname)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("file %q is not regular", fname)
	}
	ll.Debugw("file information", "size", info.Size())

	return nil
}
