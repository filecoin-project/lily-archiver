package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ipfs/go-ipfs-api"
)

func announceExport(ctx context.Context, em *ExportManifest, outputPath string, sh *shell.Shell) error {
	for _, ef := range em.Files {
		if ef.Announced {
			continue
		}
		if err := announceFile(ctx, ef, outputPath, sh); err != nil {
			return err
		}
	}

	return nil
}

func announceFile(ctx context.Context, ef ExportFile, outputPath string, sh *shell.Shell) error {
	ll := logger.With("table", ef.TableName, "date", ef.Date.String())
	ll.Info("announcing export file")

	shipFile := filepath.Join(outputPath, ef.Path())
	_, err := os.Stat(shipFile)
	if err != nil {
		return fmt.Errorf("file %q stat error: %w", shipFile, err)
	}

	f, err := os.Open(shipFile) // For read access.
	if err != nil {
		return fmt.Errorf("open shipped file: %w", err)
	}
	defer f.Close()

	ll.Debug("adding to ipfs")

	fileHash, err := sh.Add(f, shell.Pin(true), shell.CidVersion(1), shell.RawLeaves(true))
	if err != nil {
		return fmt.Errorf("add file to ipfs: %w", err)
	}
	ll.Infof("published with hash %s", fileHash)

	mfsPath := filepath.Join("/", ef.Path())
	mfsDir := filepath.Dir(mfsPath)

	if err := sh.FilesMkdir(ctx, mfsDir, shell.CidVersion(1), shell.FilesMkdir.Parents(true)); err != nil {
		return fmt.Errorf("mfs mkdir %s: %w", mfsDir, err)
	}

	if err := sh.FilesCp(ctx, "/ipfs/"+fileHash, mfsPath); err != nil {
		return fmt.Errorf("mfs cp: %w", err)
	}

	mfsHash, err := sh.FilesFlush(ctx, mfsPath)
	if err != nil {
		return fmt.Errorf("mfs flush: %w", err)
	}

	ll.Infof("mfsHash: %s", mfsHash)

	return nil
}
