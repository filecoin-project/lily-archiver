package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

type Compression struct {
	Names      []string
	Extension  string
	Executable string
	CommandFn  func(source string, destination string) string
}

var CompressionList = []Compression{
	{
		Names:      []string{"gzip", "gz"},
		Extension:  "gz",
		Executable: "gzip",
		CommandFn: func(source string, destination string) string {
			return fmt.Sprintf("gzip --no-name --rsyncable --stdout %s > %s", source, destination)
		},
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

func verifyShipDependencies(shipPath string, c Compression) error {
	// Check compression executable is available
	_, err := exec.LookPath(c.Executable)
	if err != nil {
		return fmt.Errorf("missing %s executable: %w", c.Executable, err)
	}

	// Check ship path exists and is a directory
	info, err := os.Stat(shipPath)
	if err != nil {
		return fmt.Errorf("stat ship path: %w", err)
	}

	if !info.Mode().IsDir() {
		return fmt.Errorf("ship path is not a directory")
	}

	return nil
}

func shipExportFile(ctx context.Context, ef *ExportFile, wi WalkInfo, shipPath string) error {
	ll := logger.With("table", ef.TableName, "date", ef.Date.String())
	ll.Info("shipping export file")

	walkFile := wi.WalkFile(ef.TableName)

	info, err := os.Stat(walkFile)
	if err != nil {
		return fmt.Errorf("file %q stat error: %w", walkFile, err)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("file %q is not regular", walkFile)
	}
	ll.Debugf("found export file %s", walkFile)

	shipFile := filepath.Join(shipPath, ef.Path())

	filePath := filepath.Dir(shipFile)
	if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
		return fmt.Errorf("mkdir %q: %w", filePath, err)
	}

	ll.Debugf("compressing to %s", shipFile)
	bashcmd := ef.Compression.CommandFn(walkFile, shipFile)

	cmd := exec.Command("bash", "-c", bashcmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		ll.Errorf("compression failed: %v", err)
		ll.Errorf("command used: %s", bashcmd)
		ll.Errorf("stderr: %s", stderr.String())
		return fmt.Errorf("compression: %w", err)
	}

	if _, err := os.Stat(shipFile); err != nil {
		return fmt.Errorf("file %q stat error: %w", shipFile, err)
	}

	return nil
}
