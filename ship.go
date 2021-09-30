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

func verifyShipDependencies() error {
	// Check gzip is available
	_, err := exec.LookPath("gzip")
	if err != nil {
		return fmt.Errorf("missing gzip executable: %w", err)
	}
	return nil
}

func shipExport(ctx context.Context, em *ExportManifest, wi WalkInfo, outputPath string) error {
	for _, ef := range em.Files {
		if ef.Shipped {
			continue
		}
		if err := shipFile(ctx, ef, wi, outputPath); err != nil {
			return err
		}

		// Ensure that each newly shipped file is marked for announcement. In future we could
		// avoid doing this if we know file has not changed (e.g. was just regenerated)
		ef.Announced = false
	}

	return nil
}

func shipFile(ctx context.Context, ef *ExportFile, wi WalkInfo, outputPath string) error {
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

	shipFile := filepath.Join(outputPath, ef.Path())

	shipPath := filepath.Dir(shipFile)
	if err := os.MkdirAll(shipPath, os.ModePerm); err != nil {
		return fmt.Errorf("mkdir %q: %w", shipPath, err)
	}

	ll.Debugf("compressing to %s", shipFile)
	bashcmd := fmt.Sprintf("gzip --no-name --rsyncable --stdout %s > %s", walkFile, shipFile)

	cmd := exec.Command("bash", "-c", bashcmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		ll.Errorf("gzip failed: %v", err)
		ll.Errorf("stderr: %s", stderr.String())
		return fmt.Errorf("gzip: %w", err)
	}

	if _, err := os.Stat(shipFile); err != nil {
		return fmt.Errorf("file %q stat error: %w", shipFile, err)
	}

	return nil
}
