package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lily/commands"
	"github.com/filecoin-project/lily/lens/lily"
	"github.com/filecoin-project/lily/schedule"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var ErrJobNotFound = errors.New("job not found")

type ExportManifest struct {
	Period  ExportPeriod
	Network string
	Files   []*ExportFile
}

func manifestForPeriod(ctx context.Context, p ExportPeriod, network string, genesisTs int64, shipPath string, schemaVersion int, allowedTables []Table, compression Compression) (*ExportManifest, error) {
	em := &ExportManifest{
		Period:  p,
		Network: network,
	}

	networkVersions := NetworkVersionsBetweenHeights(abi.ChainEpoch(p.StartHeight), abi.ChainEpoch(p.EndHeight))

	for _, t := range TablesBySchema[schemaVersion] {
		allowed := false
		for i := range allowedTables {
			if allowedTables[i].Name == t.Name {
				allowed = true
				break
			}
		}
		if !allowed {
			continue
		}

		expected := false
		for _, nv := range networkVersions {
			if t.NetworkVersionRange.From <= nv && t.NetworkVersionRange.To >= nv {
				expected = true
				break
			}
		}
		if !expected {
			continue
		}

		f := ExportFile{
			Date:        em.Period.Date,
			Schema:      schemaVersion,
			Network:     network,
			TableName:   t.Name,
			Format:      "csv", // hardcoded for now
			Compression: compression,
			Shipped:     true,
			Cid:         cid.Undef,
		}

		_, err := os.Stat(filepath.Join(shipPath, f.Path()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				f.Shipped = false
			} else {
				return nil, fmt.Errorf("stat: %w", err)
			}
		}

		em.Files = append(em.Files, &f)
	}

	return em, nil
}

func (em *ExportManifest) FilterTables(allowed []Table) *ExportManifest {
	out := new(ExportManifest)
	out.Period = em.Period

	for _, f := range em.Files {
		include := false
		for _, t := range allowed {
			if f.TableName == t.Name {
				include = true
				break
			}
		}

		if include {
			out.Files = append(out.Files, f)
		}
	}

	return out
}

func (em *ExportManifest) HasUnshippedFiles() bool {
	for _, f := range em.Files {
		if !f.Shipped {
			return true
		}
	}
	return false
}

func (em *ExportManifest) FilesForTask(task string) []*ExportFile {
	var files []*ExportFile
	for _, ef := range em.Files {
		table, ok := TablesByName[ef.TableName]
		if !ok {
			// weird if we have an unknown table, but doesn't warrant exiting here
			continue
		}
		if table.Task == task {
			files = append(files, ef)
		}
	}

	return files
}

// ExportPeriod holds the parameters for an export covering a date.
type ExportPeriod struct {
	Date        Date
	StartHeight int64
	EndHeight   int64
}

// Next returns the following export period which cover the next calendar day.
func (e *ExportPeriod) Next() ExportPeriod {
	return ExportPeriod{
		Date:        e.Date.Next(),
		StartHeight: e.EndHeight + 1,
		EndHeight:   e.EndHeight + EpochsInDay,
	}
}

// firstExportPeriod returns the first period that should be exported. This is the period covering the day
// from genesis to 23:59:59 UTC the same day.
func firstExportPeriod(genesisTs int64) ExportPeriod {
	genesisDt := time.Unix(genesisTs, 0).UTC()
	midnightEpochAfterGenesis := midnightEpochForTs(genesisDt.AddDate(0, 0, 1).Unix(), genesisTs)

	return ExportPeriod{
		Date: Date{
			Year:  genesisDt.Year(),
			Month: int(genesisDt.Month()),
			Day:   genesisDt.Day(),
		},
		StartHeight: 0,
		EndHeight:   midnightEpochAfterGenesis - 1,
	}
}

// firstExportPeriodAfter returns the first period that should be exported at or after a specified minimum height.
func firstExportPeriodAfter(minHeight int64, genesisTs int64) ExportPeriod {
	// Iteration here guarantees we are always consistent with height ranges
	p := firstExportPeriod(genesisTs)
	for p.StartHeight < minHeight {
		p = p.Next()
	}

	return p
}

// Returns the height at midnight UTC (the start of the day) on the given date
func midnightEpochForTs(ts int64, genesisTs int64) int64 {
	t := time.Unix(ts, 0).UTC()
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return UnixToHeight(midnight.Unix(), genesisTs)
}

type ExportFile struct {
	Date        Date
	Schema      int
	Network     string
	TableName   string
	Format      string
	Compression Compression
	Shipped     bool // Shipped indicates that the file has been compressed and placed in the shared filesystem
	Cid         cid.Cid
	Unique      bool // Ensures that no two files will ever be named the same; see sentinel-archiver#11
}

// Path returns the path and file name that the export file should be written to.
func (e *ExportFile) Path() string {
	filename := e.Filename()
	return filepath.Join(e.Network, e.Format, strconv.Itoa(e.Schema), e.TableName, strconv.Itoa(e.Date.Year), filename)
}

// Filename returns file name that the export file should be written to.
func (e *ExportFile) Filename() string {
	if e.Unique {
		return fmt.Sprintf("%s-%s-%s.%s.%s", e.TableName, e.Date.String(), uuid.New(), e.Format, e.Compression.Extension)
	}
	return fmt.Sprintf("%s-%s.%s.%s", e.TableName, e.Date.String(), e.Format, e.Compression.Extension)
}

func (e *ExportFile) String() string {
	return fmt.Sprintf("%s-%s", e.TableName, e.Date.String())
}

// tasksForManifest calculates the visor tasks needed to produce the unshipped files in the supplied manifest
func tasksForManifest(em *ExportManifest) []string {
	tasks := make(map[string]struct{}, 0)

	for _, f := range em.Files {
		if f.Shipped {
			continue
		}
		t := TablesByName[f.TableName]
		tasks[t.Task] = struct{}{}
	}

	tasklist := make([]string, 0, len(tasks))
	for task := range tasks {
		tasklist = append(tasklist, task)
	}

	return tasklist
}

// walkForManifest creates a walk configuration for the given manifest
func walkForManifest(em *ExportManifest) (*lily.LilyWalkConfig, error) {
	walkName, err := unusedWalkName(storageConfig.path, em.Period.Date.String())
	if err != nil {
		return nil, fmt.Errorf("walk name: %w", err)
	}

	tasks := tasksForManifest(em)

	// Ensure we always produce the chain_consenus table
	hasConsensusTask := false
	for _, task := range tasks {
		if task == "consensus" {
			hasConsensusTask = true
			break
		}
	}
	if !hasConsensusTask {
		tasks = append(tasks, "consensus")
	}

	return &lily.LilyWalkConfig{
		JobConfig: lily.LilyJobConfig{
			Name:                walkName,
			Tasks:               tasks,
			Window:              0, // no time out
			RestartDelay:        0,
			RestartOnCompletion: false,
			RestartOnFailure:    false,
			Storage:             storageConfig.name,
		},
		From: em.Period.StartHeight,
		To:   em.Period.EndHeight,
	}, nil
}

func unusedWalkName(exportPath, suffix string) (string, error) {
	walkName := fmt.Sprintf("arch%s-%s", time.Now().UTC().Format("0102"), suffix)
	for i := 0; i < 500; i++ {
		fname := exportFilePath(exportPath, walkName, "visor_processing_reports")
		_, err := os.Stat(fname)
		if errors.Is(err, os.ErrNotExist) {
			return walkName, nil
		}
		walkName = fmt.Sprintf("arch%s-%d-%s", time.Now().UTC().Format("0102"), rand.Intn(10000), suffix)
	}

	return "", fmt.Errorf("failed to find unusued walk name in a reasonable time")
}

// exportFilePath returns the full path to an export file
func exportFilePath(exportPath, prefix, name string) string {
	return filepath.Join(exportPath, fmt.Sprintf("%s-%s.csv", prefix, name))
}

func processExport(ctx context.Context, em *ExportManifest, shipPath string) error {
	ll := logger.With("date", em.Period.Date.String(), "from", em.Period.StartHeight, "to", em.Period.EndHeight)

	if !em.HasUnshippedFiles() {
		ll.Info("all files shipped, nothing to do")
		return nil
	}

	for _, f := range em.Files {
		if !f.Shipped {
			ll.Debugf("missing table %s", f.TableName)
		}
	}

	exportStartHeightGauge.Set(float64(em.Period.EndHeight + Finality))
	ll.Info("preparing to export files for shipping")

	// We must wait for one full finality after the end of the period before running the export
	earliestStartTs := HeightToUnix(em.Period.EndHeight+Finality, networkConfig.genesisTs)
	if time.Now().Unix() < earliestStartTs {
		ll.Infof("cannot start export until %s", time.Unix(earliestStartTs, 0).UTC().Format(time.RFC3339))
	}
	if err := WaitUntil(ctx, timeIsAfter(earliestStartTs), 0, time.Second*30); err != nil {
		return fmt.Errorf("failed waiting for earliest export time: %w", err)
	}

	if err := WaitUntil(ctx, lilyIsSyncedToEpoch(lilyConfig.apiAddr, lilyConfig.apiToken, em, ll), 0, time.Minute*10); err != nil {
		return fmt.Errorf("failed waiting for lily to sync to required epoch: %w", err)
	}

	processExportStartedCounter.Inc()
	processExportInProgressGauge.Set(1)
	defer func() {
		processExportInProgressGauge.Set(0)
	}()

	var wi WalkInfo
	if err := WaitUntil(ctx, walkIsCompleted(lilyConfig.apiAddr, lilyConfig.apiToken, em, &wi, ll), 0, time.Second*30); err != nil {
		return fmt.Errorf("failed performing walk: %w", err)
	}

	ll.Info("export complete")
	report, err := verifyTasks(ctx, wi, tasksForManifest(em))
	if err != nil {
		return fmt.Errorf("failed to verify export files: %w", err)
	}

	shipFailure := false
	for task, ts := range report.TaskStatus {
		if !ts.IsOK() {
			verifyTableErrorsCounter.Inc()
			shipFailure = true
			continue
		}

		files := em.FilesForTask(task)
		for _, ef := range files {
			if !ef.Shipped {
				if err := shipExportFile(ctx, ef, wi, shipPath); err != nil {
					shipTableErrorsCounter.Inc()
					shipFailure = true
					ll.Errorw("failed to ship export file", "error", err)
					continue
				}

				if err := removeExportFile(ctx, ef, wi); err != nil {
					ll.Errorw("failed to remove export file", "error", err, "file", wi.WalkFile(ef.TableName))
				}
			}
		}
	}

	if shipFailure {
		return fmt.Errorf("failed to ship one or more export files")
	}

	return nil
}

func processRangedExport(ctx context.Context, em *ExportManifest, shipPath string) error {
	ll := logger.With("date", em.Period.Date.String(), "from", em.Period.StartHeight, "to", em.Period.EndHeight)

	for _, f := range em.Files {
		// since we want to ensure uniqueness in ranged exports
		// and not overwrite existing or ignore existing exports,
		// this should be false; see sentinel-archiver#11
		f.Shipped = false
	}

	exportStartHeightGauge.Set(float64(em.Period.EndHeight))
	ll.Info("preparing to export files for shipping")

	if err := WaitUntil(ctx, lilyIsSyncedToEpoch(lilyConfig.apiAddr, lilyConfig.apiToken, em, ll), 0, time.Minute*10); err != nil {
		return fmt.Errorf("failed waiting for lily to sync to required epoch: %w", err)
	}

	processExportStartedCounter.Inc()
	processExportInProgressGauge.Set(1)
	defer func() {
		processExportInProgressGauge.Set(0)
	}()

	var wi WalkInfo
	if err := WaitUntil(ctx, walkIsCompleted(lilyConfig.apiAddr, lilyConfig.apiToken, em, &wi, ll), 0, time.Second*30); err != nil {
		return fmt.Errorf("failed performing walk: %w", err)
	}

	ll.Info("export complete")
	report, err := verifyTasks(ctx, wi, tasksForManifest(em))
	if err != nil {
		return fmt.Errorf("failed to verify export files: %w", err)
	}

	shipFailure := false
	for task, ts := range report.TaskStatus {
		if !ts.IsOK() {
			verifyTableErrorsCounter.Inc()
			shipFailure = true
			continue
		}

		files := em.FilesForTask(task)
		for _, ef := range files {
			// ensure that there will be different files
			// see sentinel-archiver#11
			ef.Unique = true
			if !ef.Shipped {
				if err := shipExportFile(ctx, ef, wi, shipPath); err != nil {
					shipTableErrorsCounter.Inc()
					shipFailure = true
					ll.Errorw("failed to ship export file", "error", err)
					continue
				}

				if err := removeExportFile(ctx, ef, wi); err != nil {
					ll.Errorw("failed to remove export file", "error", err, "file", wi.WalkFile(ef.TableName))
				}
			}
		}
	}

	if shipFailure {
		return fmt.Errorf("failed to ship one or more export files")
	}

	return nil
}

func exportIsProcessed(p ExportPeriod, allowedTables []Table, compression Compression, shipPath string) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		em, err := manifestForPeriod(ctx, p, networkConfig.name, networkConfig.genesisTs, shipPath, storageConfig.schemaVersion, allowedTables, compression)
		if err != nil {
			processExportErrorsCounter.Inc()
			logger.Errorw("failed to create manifest", "error", err, "date", p.Date.String())
			return false, nil // force a retry
		}

		if err := processExport(ctx, em, shipPath); err != nil {
			processExportErrorsCounter.Inc()
			ll := logger.With("date", em.Period.Date.String(), "from", em.Period.StartHeight, "to", em.Period.EndHeight)
			ll.Errorw("failed to process export", "error", err)
			return false, nil // force a retry
		}

		return true, nil
	}
}

type basicLogger interface {
	Info(...interface{})
	Infof(string, ...interface{})
	Infow(string, ...interface{})
	Debug(...interface{})
	Debugf(string, ...interface{})
	Debugw(string, ...interface{})
	Error(...interface{})
	Errorf(string, ...interface{})
	Errorw(string, ...interface{})
}

func walkIsCompleted(apiAddr string, apiToken string, em *ExportManifest, walkInfo *WalkInfo, ll basicLogger) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		walkCfg, err := walkForManifest(em)
		if err != nil {
			walkErrorsCounter.Inc()
			ll.Errorf("failed to create walk configuration: %v")
			return false, nil
		}
		ll.Debugw(fmt.Sprintf("using tasks %s", strings.Join(walkCfg.JobConfig.Tasks, ",")), "walk", walkCfg.JobConfig.Name)

		var jobID schedule.JobID
		ll.Infow("starting walk", "walk", walkCfg.JobConfig.Name)
		if err := WaitUntil(ctx, jobHasBeenStarted(lilyConfig.apiAddr, lilyConfig.apiToken, walkCfg, &jobID, ll), 0, time.Second*30); err != nil {
			walkErrorsCounter.Inc()
			ll.Errorw(fmt.Sprintf("failed starting walk: %v", err), "walk", walkCfg.JobConfig.Name)
			return false, nil
		}

		ll.Infow("waiting for walk to complete", "walk", walkCfg.JobConfig.Name, "job_id", jobID)
		if err := WaitUntil(ctx, jobHasEnded(lilyConfig.apiAddr, lilyConfig.apiToken, jobID, ll), time.Second*30, time.Second*30); err != nil {
			walkErrorsCounter.Inc()
			ll.Errorw(fmt.Sprintf("failed waiting for walk to finish: %v", err), "walk", walkCfg.JobConfig.Name, "job_id", jobID)
			return false, nil
		}

		ll.Infow("walk complete", "walk", walkCfg.JobConfig.Name, "job_id", jobID)
		var jobListRes schedule.JobListResult
		if err := WaitUntil(ctx, jobGetResult(lilyConfig.apiAddr, lilyConfig.apiToken, walkCfg.JobConfig.Name, jobID, &jobListRes, ll), 0, time.Second*30); err != nil {
			walkErrorsCounter.Inc()
			ll.Errorw(fmt.Sprintf("failed waiting walk result: %v", err), "walk", walkCfg.JobConfig.Name, "job_id", jobID)
			return false, nil
		}

		if jobListRes.Error != "" {
			walkErrorsCounter.Inc()
			ll.Errorw(fmt.Sprintf("walk failed: %s", jobListRes.Error), "walk", walkCfg.JobConfig.Name, "job_id", jobID)
			return false, nil
		}

		wi := WalkInfo{
			Name:   walkCfg.JobConfig.Name,
			Path:   storageConfig.path,
			Format: "csv",
		}
		err = touchExportFiles(ctx, em, wi)
		if err != nil {
			walkErrorsCounter.Inc()
			ll.Errorw(fmt.Sprintf("failed to touch export files: %v", err), "walk", walkCfg.JobConfig.Name)
			return false, nil
		}

		*walkInfo = wi
		return true, nil
	}
}

func jobHasEnded(apiAddr string, apiToken string, id schedule.JobID, ll basicLogger) func(context.Context) (bool, error) {
	// To ensure this is robust in the case of a lily node restarting or being temporarily unresponsive, this
	// function opens its own api connection and never returns an error unless the job cannot be found
	return func(ctx context.Context) (bool, error) {
		api, closer, err := getLilyAPI(ctx, apiAddr, apiToken)
		if err != nil {
			lilyConnectionErrorsCounter.Inc()
			ll.Errorf("failed to connect to lily api at %s: %v", apiAddr, err)
			return false, nil
		}
		defer closer()

		jr, err := getJobResult(ctx, api, id)
		if err != nil {
			lilyJobErrorsCounter.Inc()
			if errors.Is(err, ErrJobNotFound) {
				return false, err
			}
			ll.Errorf("failed to get job result", "error", err, "job_id", id)
			return false, nil
		}

		if jr.Running {
			return false, nil
		}
		return true, nil
	}
}

// note: jobID is an out parameter and the name in walkCfg may be updated with an existing name
func jobHasBeenStarted(apiAddr string, apiToken string, walkCfg *lily.LilyWalkConfig, jobID *schedule.JobID, ll basicLogger) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		api, closer, err := getLilyAPI(ctx, apiAddr, apiToken)
		if err != nil {
			lilyConnectionErrorsCounter.Inc()
			ll.Errorf("failed to connect to lily api at %s: %v", apiAddr, err)
			return false, nil
		}
		defer closer()

		// Check if walk is already running
		jr, err := findExistingJob(ctx, api, walkCfg)
		if err != nil {
			if !errors.Is(err, ErrJobNotFound) {
				lilyJobErrorsCounter.Inc()
				ll.Errorw("failed to read jobs", "error", err)
				return false, nil
			}

			// No existing job, start a new walk
			res, err := api.LilyWalk(ctx, walkCfg)
			if err != nil {
				ll.Errorw("failed to create walk", "error", err, "walk", walkCfg.JobConfig.Name)
				return false, nil
			}

			// we're done
			*jobID = res.ID
			return true, err

		}

		ll.Infow("adopting running walk that matched required job", "job_id", jr.ID, "walk", jr.Name)
		*jobID = jr.ID
		walkCfg.JobConfig.Name = jr.Name
		return true, nil
	}
}

func jobGetResult(apiAddr string, apiToken string, walkName string, walkID schedule.JobID, jobListRes *schedule.JobListResult, ll basicLogger) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		api, closer, err := getLilyAPI(ctx, apiAddr, apiToken)
		if err != nil {
			lilyConnectionErrorsCounter.Inc()
			ll.Errorf("failed to connect to lily api at %s: %v", apiAddr, err)
			return false, nil
		}
		defer closer()

		res, err := getJobResult(ctx, api, walkID)
		if err != nil {
			lilyJobErrorsCounter.Inc()
			ll.Errorf("failed reading job result for walk %s with id %d: %v", walkName, walkID, err)
			return false, nil
		}
		*jobListRes = *res
		return true, nil
	}
}

func timeIsAfter(targetTs int64) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		nowTs := time.Now().Unix()
		return nowTs > targetTs, nil
	}
}

func getJobResult(ctx context.Context, api lily.LilyAPI, id schedule.JobID) (*schedule.JobListResult, error) {
	jobs, err := api.LilyJobList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}

	for _, jr := range jobs {
		if jr.ID == id {
			return &jr, nil
		}
	}

	return nil, ErrJobNotFound
}

func findExistingJob(ctx context.Context, api lily.LilyAPI, walkCfg *lily.LilyWalkConfig) (*schedule.JobListResult, error) {
	jobs, err := api.LilyJobList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}

	for _, jr := range jobs {
		if jr.Type != "walk" {
			continue
		}

		if !jr.Running {
			continue
		}

		if jr.Params["storage"] != walkCfg.JobConfig.Storage {
			continue
		}

		from, err := strconv.ParseInt(jr.Params["minHeight"], 10, 64)
		if err != nil || from != walkCfg.From {
			continue
		}

		to, err := strconv.ParseInt(jr.Params["maxHeight"], 10, 64)
		if err != nil || to != walkCfg.To {
			continue
		}

		if !stringSliceContainsAll(jr.Tasks, walkCfg.JobConfig.Tasks) {
			continue
		}

		return &jr, nil
	}

	return nil, ErrJobNotFound
}

type WalkInfo struct {
	Name   string // name of walk
	Path   string // storage output path
	Format string // usually csv
}

// WalkFile returns the path to the file that the walk would write for the given table
func (w *WalkInfo) WalkFile(table string) string {
	return filepath.Join(w.Path, fmt.Sprintf("%s-%s.%s", w.Name, table, w.Format))
}

// touchExportFiles ensures we have a zero length file for every export we expect from lily
func touchExportFiles(ctx context.Context, em *ExportManifest, wi WalkInfo) error {
	for _, ef := range em.Files {
		walkFile := wi.WalkFile(ef.TableName)
		f, err := os.OpenFile(walkFile, os.O_APPEND|os.O_CREATE, DefaultFilePerms)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		f.Close()
	}

	return nil
}

func removeExportFile(ctx context.Context, ef *ExportFile, wi WalkInfo) error {
	walkFile := wi.WalkFile(ef.TableName)
	if err := os.Remove(walkFile); err != nil {
		return fmt.Errorf("remove: %w", err)
	}

	return nil
}

func stringSliceContainsAll(haystack, needles []string) bool {
	if len(haystack) < len(needles) {
		return false
	}

	for _, needle := range needles {
		found := false
		for i := range haystack {
			if haystack[i] == needle {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func lilyIsSyncedToEpoch(apiAddr string, apiToken string, em *ExportManifest, ll basicLogger) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		api, closer, err := getLilyAPI(ctx, apiAddr, apiToken)
		if err != nil {
			lilyConnectionErrorsCounter.Inc()
			ll.Errorf("failed to connect to lily api at %s: %v", apiAddr, err)
			return false, nil
		}
		defer closer()

		height, err := getLilyChainHeight(ctx, api)
		if err != nil {
			ll.Errorw(fmt.Sprintf("failed to get chain head: %v", err))
			return false, nil
		}

		if height < em.Period.EndHeight {
			ll.Infow("lily is not synced to necessary epoch", "lily_height", height)
			return false, nil
		}

		return true, nil
	}
}

func getLilyChainHeight(ctx context.Context, api lily.LilyAPI) (int64, error) {
	ts, err := api.ChainHead(ctx)
	if err != nil {
		return -1, err
	}

	return int64(ts.Height()), nil
}

type closerFunc func()

func getLilyAPI(ctx context.Context, apiAddr string, apiToken string) (lily.LilyAPI, closerFunc, error) {
	dialAddr, err := apiDialAddr(apiAddr, "v0")
	if err != nil {
		return nil, nil, fmt.Errorf("api dial addr: %v", err)
	}

	authHeader := apiAuthHeader(apiToken)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			api, closer, err := commands.NewSentinelNodeRPC(ctx, dialAddr, authHeader)
			if err == nil {
				return api, closerFunc(closer), nil
			}
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

func apiDialAddr(addr string, version string) (string, error) {
	ma, err := multiaddr.NewMultiaddr(addr)
	if err == nil {
		_, daddr, err := manet.DialArgs(ma)
		if err != nil {
			return "", err
		}

		return "ws://" + daddr + "/rpc/" + version, nil
	}

	_, err = url.Parse(addr)
	if err != nil {
		return "", err
	}
	return addr + "/rpc/" + version, nil
}

func apiAuthHeader(token string) http.Header {
	if len(token) != 0 {
		headers := http.Header{}
		headers.Add("Authorization", "Bearer "+token)
		return headers
	}
	return nil
}
