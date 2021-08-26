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

func shipExport(ctx context.Context, em *ExportManifest, wi WalkInfo, outputPath string, p *Peer) error {
	for _, ef := range em.Files {
		if err := shipFile(ctx, ef, wi, outputPath, p); err != nil {
			return err
		}
	}

	return nil
}

func shipFile(ctx context.Context, ef ExportFile, wi WalkInfo, outputPath string, p *Peer) error {
	_, err := exec.LookPath("gzip")
	if err != nil {
		return fmt.Errorf("gzip executable: %w", err)
	}

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
	bashcmd := fmt.Sprintf("gzip --stdout %s > %s", walkFile, shipFile)

	cmd := exec.Command("bash", "-c", bashcmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		ll.Errorf("gzip failed: %v", err)
		ll.Errorf("stderr: %s", stderr.String())
		return fmt.Errorf("gzip: %w", err)
	}

	ginfo, err := os.Stat(shipFile)
	if err != nil {
		return fmt.Errorf("file %q stat error: %w", shipFile, err)
	}
	ll.Debugf("compressed file size: %d", ginfo.Size())

	f, err := os.Open(shipFile) // For read access.
	if err != nil {
		return fmt.Errorf("open shipped file: %w", err)
	}
	defer f.Close()

	ll.Debug("adding to ipfs")

	n, err := p.AddFile(ctx, f)
	if err != nil {
		return fmt.Errorf("add file to ipfs: %w", err)
	}
	ll.Infof("published with cid %s", n.Cid())
	return nil
}
