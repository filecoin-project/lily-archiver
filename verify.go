package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/filecoin-project/lily/model/visor"
)

func verifyExport(ctx context.Context, em *ExportManifest, wi WalkInfo, shipPath string) (*VerificationReport, error) {
	tasks := make(map[string]struct{}, 0)
	for _, ef := range em.Files {
		t, ok := TablesByName[ef.TableName]
		if !ok {
			return nil, fmt.Errorf("unknown table %q", ef.TableName)
		}
		tasks[t.Task] = struct{}{}
	}

	tasklist := make([]string, 0, len(tasks))
	for task := range tasks {
		tasklist = append(tasklist, task)
	}

	return verifyTasks(ctx, wi, tasklist)
}

func verifyTasks(ctx context.Context, wi WalkInfo, tasks []string) (*VerificationReport, error) {
	consensusPath := wi.WalkFile("chain_consensus")
	consensusFile, err := os.Open(consensusPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open consensus export: %w", err)
	}
	defer consensusFile.Close()

	heights := map[int64][]string{}
	r := csv.NewReader(bufio.NewReader(consensusFile))
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("consensus export: read: %w", err)
		}

		if len(row) < 4 {
			return nil, fmt.Errorf("consensus export: row has too few columns") // TODO: line number
		}

		height, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("consensus export: malformed height: %w", err)
		}

		var blocks []string
		if len(row[3]) > 2 {
			blocks = strings.Split(row[3][1:len(row[3])-1], ",")
		}
		heights[height] = blocks
	}
	logger.Infof("expecting %d heights", len(heights))

	type taskInfo struct {
		status TaskStatus
		seen   map[int64]bool
	}

	taskInfos := map[string]taskInfo{}
	for _, task := range tasks {
		info := taskInfo{
			seen: map[int64]bool{},
		}
		for height, blocks := range heights {
			if blocks != nil {
				info.seen[height] = false
			}
		}
		taskInfos[task] = info
	}

	reportsPath := wi.WalkFile("visor_processing_reports")
	reportsFile, err := os.Open(reportsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open processing reports: %w", err)
	}
	defer reportsFile.Close()

	r = csv.NewReader(bufio.NewReader(reportsFile))
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("processing reports: read: %w", err)
		}

		if len(row) < 9 {
			return nil, fmt.Errorf("processing reports: row has too few columns") // TODO: row number
		}

		task := row[3]
		info, wanted := taskInfos[task]
		if !wanted {
			continue
		}
		ll := logger.With("task", task)

		height, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("processing reports: malformed height: %w", err)
		}

		alreadySeen, expecting := info.seen[height]
		if !expecting {
			ll.Infof("unexpected data found for height %d", height)
			info.status.Unexpected = append(info.status.Unexpected, height)
		} else {
			if alreadySeen {
				// TODO: record this so it can be cleaned up?
				ll.Debugf("duplicate data found found for height %d", height)
			}
			info.seen[height] = true

			switch row[6] {
			case visor.ProcessingStatusOK:
				continue
			case visor.ProcessingStatusInfo:
				ll.Infof("info status %s found for height %d", row[7], height)
			case visor.ProcessingStatusError:
				ll.Infof("error found for height %d: %v", height, row[8])
				info.status.Error = append(info.status.Error, height)
			case visor.ProcessingStatusSkip:
				ll.Infof("skip found for height %d", height)
				info.status.Missing = append(info.status.Missing, height)

			default:
				ll.Infof("unknown status %s for height", row[6], height)
				// TODO: abort
			}
		}

		taskInfos[task] = info

	}

	report := VerificationReport{
		TaskStatus: map[string]TaskStatus{},
	}
	for task, info := range taskInfos {
		for height, seen := range info.seen {
			if !seen {
				info.status.Missing = append(info.status.Missing, height)
				logger.With("task", task).Infof("data not found for height %d", height)
			}
		}

		sort.Slice(info.status.Missing, func(a, b int) bool { return info.status.Missing[a] < info.status.Missing[b] })
		sort.Slice(info.status.Error, func(a, b int) bool { return info.status.Error[a] < info.status.Error[b] })
		sort.Slice(info.status.Unexpected, func(a, b int) bool { return info.status.Unexpected[a] < info.status.Unexpected[b] })
		report.TaskStatus[task] = info.status
	}

	return &report, nil
}

type VerificationReport struct {
	TaskStatus map[string]TaskStatus
}

type TaskStatus struct {
	Missing    []int64 // heights that were missing or skipped
	Error      []int64 // heights that reported an error
	Unexpected []int64 // heights that should not have been present
}

type Range struct {
	Lower int64
	Upper int64
}

func ranges(hs []int64) []Range {
	var rs []Range

	if len(hs) > 0 {
		start, end := hs[0], hs[0]
		for i := 1; i < len(hs); i++ {
			if hs[i] == end+1 {
				end = hs[i]
				continue
			}
			rs = append(rs, Range{Lower: start, Upper: end})
			start, end = hs[i], hs[i]

		}
		rs = append(rs, Range{Lower: start, Upper: end})
	}

	return rs
}
