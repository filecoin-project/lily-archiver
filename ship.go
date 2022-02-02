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

	"github.com/filecoin-project/lily/model"
	"github.com/filecoin-project/lily/schemas/v1"
	"github.com/filecoin-project/lily/storage"
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
	return ensureHeaderFiles(shipPath, tables)
}

func ensureHeaderFiles(shipPath string, tables []Table) error {
	// Check header path exists and is a directory
	headerBasePath := filepath.Join(shipPath, networkConfig.name, "csv", strconv.Itoa(storageConfig.schemaVersion))
	if err := os.MkdirAll(headerBasePath, DefaultDirPerms); err != nil {
		return fmt.Errorf("mkdir %q: %w", headerBasePath, err)
	}

	info, err := os.Stat(headerBasePath)
	if err != nil {
		return fmt.Errorf("stat header base path: %w", err)
	}

	if !info.Mode().IsDir() {
		return fmt.Errorf("header base path is not a directory")
	}

	var version model.Version
	switch storageConfig.schemaVersion {
	case 1:
		version = v1.Version()
	default:
		return fmt.Errorf("unknown schema version")
	}

	strg, err := storage.NewCSVStorage("", version, storage.CSVStorageOptions{})
	if err != nil {
		return fmt.Errorf("new csv storage: %w", err)
	}

	for _, table := range tables {
		headerPath := filepath.Join(headerBasePath, table.Name+".header")

		_, err := os.Stat(headerPath)
		if err == nil {
			logger.Debugf("header file exists for %s", table.Name)
			continue
		}

		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat header path (%q): %w", headerPath, err)
		}

		headers, err := strg.ModelHeaders(table.Model)
		if err != nil {
			return fmt.Errorf("generate model headers for %s: %w", table.Name, err)
		}

		if err := os.WriteFile(headerPath, []byte(strings.Join(headers, ",")), DefaultFilePerms); err != nil {
			return fmt.Errorf("write model headers for %s: %w", table.Name, err)
		}
	}

	return nil
}
