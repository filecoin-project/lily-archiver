package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/lily/commands"
	"github.com/filecoin-project/lily/lens/lily"
	"github.com/filecoin-project/lily/schedule"
	"github.com/ipfs/go-cid"
)

type Table struct {
	// Name is the name of the table
	Name string

	// Task is the name of the task that writes the table
	Task string

	// Schemas is a list of schemas for which the table is supported.
	Schemas []int
}

var TableList = []Table{
	{Name: "actor_states", Schemas: []int{0, 1}, Task: "actorstatesraw"},
	{Name: "actors", Schemas: []int{0, 1}, Task: "actorstatesraw"},
	{Name: "block_headers", Schemas: []int{0, 1}, Task: "blocks"},
	{Name: "block_messages", Schemas: []int{0, 1}, Task: "messages"},
	{Name: "block_parents", Schemas: []int{0, 1}, Task: "blocks"},
	{Name: "chain_consensus", Schemas: []int{1}, Task: "consensus"},
	{Name: "chain_economics", Schemas: []int{0, 1}, Task: "chaineconomics"},
	{Name: "chain_powers", Schemas: []int{0, 1}, Task: "actorstatespower"},
	{Name: "chain_rewards", Schemas: []int{0, 1}, Task: "actorstatesreward"},
	{Name: "derived_gas_outputs", Schemas: []int{0, 1}, Task: "messages"},
	{Name: "drand_block_entries", Schemas: []int{0, 1}, Task: "blocks"},
	{Name: "id_addresses", Schemas: []int{0, 1}, Task: "actorstatesinit"},
	{Name: "internal_messages", Schemas: []int{1}, Task: "implicitmessage"},
	{Name: "internal_parsed_messages", Schemas: []int{1}, Task: "implicitmessage"},
	{Name: "market_deal_proposals", Schemas: []int{0, 1}, Task: "actorstatesmarket"},
	{Name: "market_deal_states", Schemas: []int{0, 1}, Task: "actorstatesmarket"},
	{Name: "message_gas_economy", Schemas: []int{0, 1}, Task: "messages"},
	{Name: "messages", Schemas: []int{0, 1}, Task: "messages"},
	{Name: "miner_current_deadline_infos", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "miner_fee_debts", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "miner_infos", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "miner_locked_funds", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "miner_pre_commit_infos", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "miner_sector_deals", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "miner_sector_events", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "miner_sector_infos", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "miner_sector_posts", Schemas: []int{0, 1}, Task: "actorstatesminer"},
	{Name: "multisig_approvals", Schemas: []int{0, 1}, Task: "msapprovals"},
	{Name: "multisig_transactions", Schemas: []int{0, 1}, Task: "actorstatesmultisig"},
	{Name: "parsed_messages", Schemas: []int{0, 1}, Task: "messages"},
	{Name: "power_actor_claims", Schemas: []int{0, 1}, Task: "actorstatespower"},
	{Name: "receipts", Schemas: []int{0, 1}, Task: "messages"},
	{Name: "verified_registry_verifiers", Schemas: []int{1}, Task: "actorstatesverifreg"},
	{Name: "verified_registry_verified_clients", Schemas: []int{1}, Task: "actorstatesverifreg"},
}

var (
	// TablesByName maps a table name to the table description.
	TablesByName = map[string]Table{}

	// TablesBySchema maps a schema version to a list of tables present in that schema.
	TablesBySchema = map[int][]Table{}

	// TablesByTask maps a task name to a list of tables that are produced by that task.
	TablesByTask = map[string][]Table{}
)

func init() {
	for _, table := range TableList {
		TablesByName[table.Name] = table
		TablesByTask[table.Task] = append(TablesByTask[table.Task], table)
		for _, schema := range table.Schemas {
			TablesBySchema[schema] = append(TablesBySchema[schema], table)
		}
	}
}

type ExportManifest struct {
	Period  ExportPeriod
	Network string
	Files   []*ExportFile
}

func manifestForDate(ctx context.Context, d Date, network string, genesisTs int64, outputPath string, schemaVersion int, allowedTables []Table, peer *Peer) (*ExportManifest, error) {
	p := firstExportPeriod(genesisTs)

	if p.Date.After(d) {
		return nil, fmt.Errorf("date is before genesis: %s", d.String())
	}

	// Iteration here guarantees we are always consistent with height ranges
	for p.Date != d {
		p = p.Next()
	}

	return manifestForPeriod(ctx, p, network, genesisTs, outputPath, schemaVersion, allowedTables, peer)
}

func manifestForPeriod(ctx context.Context, p ExportPeriod, network string, genesisTs int64, outputPath string, schemaVersion int, allowedTables []Table, peer *Peer) (*ExportManifest, error) {
	em := &ExportManifest{
		Period:  p,
		Network: network,
	}

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

		f := ExportFile{
			Date:        em.Period.Date,
			Schema:      schemaVersion,
			Network:     network,
			TableName:   t.Name,
			Format:      "csv", // hardcoded for now
			Compression: "gz",  // hardcoded for now
			Shipped:     true,
			Announced:   false,
			Cid:         cid.Undef,
		}

		_, err := os.Stat(filepath.Join(outputPath, f.Path()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				f.Shipped = false
			} else {
				return nil, fmt.Errorf("stat: %w", err)
			}
		}

		exists, fcid, err := peer.fileExists(ctx, filepath.Join("/", f.Path()))
		if err != nil {
			return nil, fmt.Errorf("mfs file exists: %w", err)
		}
		f.Announced = exists
		f.Cid = fcid

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

func (em *ExportManifest) HasUnannouncedFiles() bool {
	for _, f := range em.Files {
		if !f.Announced {
			return true
		}
	}
	return false
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
	Compression string
	Shipped     bool // Shipped indicates that the file has been compressed and placed in the shared filesystem
	Announced   bool // Announced indicates that the file has added to IPFS
	Cid         cid.Cid
}

// Path returns the path and file name that the export file should be written to.
func (e *ExportFile) Path() string {
	filename := e.Filename()
	return filepath.Join(e.Network, e.Format, strconv.Itoa(e.Schema), e.TableName, strconv.Itoa(e.Date.Year), filename)
}

// Filename returns file name that the export file should be written to.
func (e *ExportFile) Filename() string {
	return fmt.Sprintf("%s-%s.%s.%s", e.TableName, e.Date.String(), e.Format, e.Compression)
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
		Name:                walkName,
		Tasks:               tasks,
		Window:              0, // no time out
		From:                em.Period.StartHeight,
		To:                  em.Period.EndHeight,
		RestartDelay:        0,
		RestartOnCompletion: false,
		RestartOnFailure:    false,
		Storage:             storageConfig.name,
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

func processExport(ctx context.Context, em *ExportManifest, outputPath string, peer *Peer) error {
	ll := logger.With("date", em.Period.Date.String(), "from", em.Period.StartHeight, "to", em.Period.EndHeight)

	if !em.HasUnshippedFiles() && !em.HasUnannouncedFiles() {
		ll.Info("all files shipped and announced, nothing to do")
		return nil
	}

	if em.HasUnshippedFiles() {
		ll.Info("preparing to export files for shipping")

		// Check that files that are not shipped are not announced in ipfs
		// This can happen if files on disk have been deleted
		for _, ef := range em.Files {
			if !ef.Shipped && ef.Announced {
				// File does not exist on disk but is in ipfs
				ll.Infof("unannouncing %s which is not shipped", ef.String())
				if err := peer.removeFile(ctx, ef); err != nil {
					return fmt.Errorf("remove announced file: %w", err)
				}
			}
		}

		// We must wait for one full finality after the end of the period before running the export
		earliestStartTs := HeightToUnix(em.Period.EndHeight+Finality, networkConfig.genesisTs)
		if time.Now().Unix() < earliestStartTs {
			ll.Infof("cannot start export until %s", time.Unix(earliestStartTs, 0).UTC().Format(time.RFC3339))
		}
		if err := WaitUntil(ctx, timeIsAfter(earliestStartTs), time.Second*30); err != nil {
			return fmt.Errorf("failed waiting for earliest export time: %w", err)
		}

		walkCfg, err := walkForManifest(em)
		if err != nil {
			return fmt.Errorf("build walk for yesterday: %w", err)
		}
		ll.Debugf("using tasks %s", strings.Join(walkCfg.Tasks, ","))

		wi := WalkInfo{
			Name:   walkCfg.Name,
			Path:   storageConfig.path,
			Format: "csv",
		}
		err = touchExportFiles(ctx, em, wi)
		if err != nil {
			return fmt.Errorf("failed to touch export files: %w", err)
		}

		api, closer, err := commands.GetAPI(ctx, lilyConfig.apiAddr, lilyConfig.apiToken)
		if err != nil {
			return fmt.Errorf("could not access lily api: %w", err)
		}
		defer closer()

		jobRes, err := api.LilyWalk(ctx, walkCfg)
		if err != nil {
			return fmt.Errorf("failed to create walk: %w", err)
		}

		ll.Infof("waiting for walk %s with id %d to complete", walkCfg.Name, jobRes.ID)
		if err := WaitUntil(ctx, jobHasEnded(api, jobRes.ID), time.Second*30); err != nil {
			return fmt.Errorf("failed waiting for job to finish: %w", err)
		}

		jr, err := getJobResult(ctx, api, jobRes.ID)
		if err != nil {
			return fmt.Errorf("failed reading job result: %w", err)
		}

		if jr.Error != "" {
			// TODO: retry
			return fmt.Errorf("job failed with error: %s", jr.Error)
		}

		ll.Info("export complete")

		_, err = verifyTasks(ctx, wi, tasksForManifest(em))
		if err != nil {
			return fmt.Errorf("failed to verify export files: %w", err)
		}

		err = shipExport(ctx, em, wi, outputPath)
		if err != nil {
			return fmt.Errorf("failed to ship export files: %w", err)
		}

		err = removeExportFiles(ctx, em, wi)
		if err != nil {
			return fmt.Errorf("failed to remove export files: %w", err)
		}

	}

	if em.HasUnannouncedFiles() {
		ll.Info("announcing shipped files")

		for _, ef := range em.Files {
			if ef.Announced {
				continue
			}
			_, err := peer.addFile(ctx, ef, outputPath)
			if err != nil {
				return fmt.Errorf("add file: %w", err)
			}
		}

		mfsCid, err := peer.rootCid()
		if err != nil {
			return fmt.Errorf("root cid: %w", err)
		}
		logger.Infof("new root cid: %s", mfsCid.String())

	}

	if err := peer.provide(ctx); err != nil {
		return fmt.Errorf("provide: %w", err)
	}

	return nil
}

func jobHasEnded(api lily.LilyAPI, id schedule.JobID) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		jr, err := getJobResult(ctx, api, id)
		if err != nil {
			return false, err
		}

		if jr.Running {
			return false, nil
		}
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

	return nil, fmt.Errorf("job %d not found", id)
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
		f, err := os.OpenFile(walkFile, os.O_APPEND|os.O_CREATE, os.ModePerm)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		f.Close()
	}

	return nil
}

func removeExportFiles(ctx context.Context, em *ExportManifest, wi WalkInfo) error {
	for _, ef := range em.Files {
		walkFile := wi.WalkFile(ef.TableName)
		if err := os.Remove(walkFile); err != nil {
			logger.Errorf("failed to remove file %s: %w", walkFile, err)
		}
	}

	return nil
}
