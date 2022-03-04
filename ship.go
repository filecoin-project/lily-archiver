package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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
		// It's ok if an export file does not exist. This can happen if there is no data or if a table has been
		// made obsolete by a network upgrade. The verify process will have already detected any errors that may
		// have prevented a table being written.
		if !errors.Is(err, os.ErrNotExist) {
			ll.Info("export file not found")
			return nil
		}
		return fmt.Errorf("file %q stat error: %w", walkFile, err)
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("file %q is not regular", walkFile)
	}
	ll.Debugf("found export file %s", walkFile)

	shipFile := filepath.Join(shipPath, ef.Path())

	filePath := filepath.Dir(shipFile)
	if err := os.MkdirAll(filePath, DefaultDirPerms); err != nil {
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

func ensureAncillaryFiles(shipPath string, tables []Table) error {
	// Ensure header files are present for tables being exported
	if err := ensureHeaderFiles(shipPath, tables); err != nil {
		return fmt.Errorf("ensure header files: %w", err)
	}

	if err := ensureSchemaFiles(shipPath, tables); err != nil {
		return fmt.Errorf("ensure schema files: %w", err)
	}
	return nil
}

func ensureHeaderFiles(shipPath string, tables []Table) error {
	for _, table := range tables {
		headerBasePath := filepath.Join(shipPath, networkConfig.name, "csv", strconv.Itoa(storageConfig.schemaVersion), table.Name)
		if _, err := os.Stat(headerBasePath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("stat header base path (%q): %w", headerBasePath, err)
			}

			if err := os.MkdirAll(headerBasePath, DefaultDirPerms); err != nil {
				return fmt.Errorf("mkdir %q: %w", headerBasePath, err)
			}
		}

		headerPath := filepath.Join(headerBasePath, table.Name+".header")

		_, err := os.Stat(headerPath)
		if err == nil {
			continue
		}

		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat header path (%q): %w", headerPath, err)
		}

		logger.Debugf("writing header file for %s", table.Name)
		headers, err := TableHeaders(table.Model)
		if err != nil {
			return fmt.Errorf("generate table headers for %s: %w", table.Name, err)
		}

		if err := os.WriteFile(headerPath, []byte(strings.Join(headers, ",")), DefaultFilePerms); err != nil {
			return fmt.Errorf("write table headers for %s: %w", table.Name, err)
		}
	}

	return nil
}

func ensureSchemaFiles(shipPath string, tables []Table) error {
	for _, table := range tables {
		schemaBasePath := filepath.Join(shipPath, networkConfig.name, "csv", strconv.Itoa(storageConfig.schemaVersion), table.Name)
		if _, err := os.Stat(schemaBasePath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("stat schema base path (%q): %w", schemaBasePath, err)
			}

			if err := os.MkdirAll(schemaBasePath, DefaultDirPerms); err != nil {
				return fmt.Errorf("mkdir %q: %w", schemaBasePath, err)
			}
		}

		schemaPath := filepath.Join(schemaBasePath, table.Name+".schema")

		_, err := os.Stat(schemaPath)
		if err == nil {
			continue
		}

		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat schema path (%q): %w", schemaPath, err)
		}

		logger.Debugf("writing schema file for %s", table.Name)
		schema, err := TableSchema(table.Model)
		if err != nil {
			return fmt.Errorf("generate table schema for %s: %w", table.Name, err)
		}

		if err := os.WriteFile(schemaPath, []byte(schema), DefaultFilePerms); err != nil {
			return fmt.Errorf("write table schema for %s: %w", table.Name, err)
		}
	}

	return nil
}
