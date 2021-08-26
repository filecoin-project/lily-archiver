package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/sentinel-visor/commands"
	"github.com/filecoin-project/sentinel-visor/lens/lily"
	"github.com/filecoin-project/sentinel-visor/schedule"
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
	Period ExportPeriod
	Files  []ExportFile
}

func manifestForDate(d Date, network string, genesisTs int64, outputPath string, schemaVersion int, allowedTables []Table) (*ExportManifest, error) {
	p := firstExportPeriod(genesisTs)

	if p.Date.After(d) {
		return nil, fmt.Errorf("date is before genesis: %s", d.String())
	}

	// Iteration here guarantees we are always consistent with height ranges
	for p.Date != d {
		p = p.Next()
	}

	return manifestForPeriod(p, network, genesisTs, outputPath, schemaVersion, allowedTables)
}

func manifestForPeriod(p ExportPeriod, network string, genesisTs int64, outputPath string, schemaVersion int, allowedTables []Table) (*ExportManifest, error) {
	em := &ExportManifest{
		Period: p,
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
		}

		_, err := os.Stat(filepath.Join(outputPath, f.Path()))
		if !errors.Is(err, os.ErrNotExist) {
			continue
		}

		em.Files = append(em.Files, f)
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

// firstExportPeriod returns the first period that should be exported. This is the period covering the day from
// from genesis to 23:59:59 UTC the same day.
func firstExportPeriod(genesisTs int64) ExportPeriod {
	genesisDt := time.Unix(genesisTs, 0)
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

// Returns the height at midnight UTC on the given date
func midnightEpochForTs(ts int64, genesisTs int64) int64 {
	t := time.Unix(ts, 0)
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
}

// Path returns the path and file name that the export file should be written to.
func (e *ExportFile) Path() string {
	filename := fmt.Sprintf("%s-%s.%s.%s", e.TableName, e.Date.String(), e.Format, e.Compression)
	return filepath.Join(e.Network, strconv.Itoa(e.Schema), e.TableName, strconv.Itoa(e.Date.Year), filename)
}

func (e *ExportFile) String() string {
	return fmt.Sprintf("%s-%s", e.TableName, e.Date.String())
}

// tasksForManifest calculates the visor tasks needed to produce the files in the supplied manifest
func tasksForManifest(em *ExportManifest) []string {
	tasks := make(map[string]struct{}, 0)

	for _, f := range em.Files {
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
	sort.Strings(tasks)
	if sort.SearchStrings(tasks, "consensus") >= len(tasks) {
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

func processExport(ctx context.Context, em *ExportManifest, outputPath string, p *Peer) error {
	ll := logger.With("date", em.Period.Date.String(), "from", em.Period.StartHeight, "to", em.Period.EndHeight)

	if len(em.Files) == 0 {
		ll.Info("found all expected files, nothing to do")
		return nil
	}

	ll.Infof("export period is missing %d files", len(em.Files))

	// We must wait for one full finality after the end of the period before running the export
	earliestStartTs := HeightToUnix(em.Period.EndHeight+Finality, networkConfig.genesisTs)
	ll.Infof("earliest time this export can start: %d (%s)", earliestStartTs, time.Unix(earliestStartTs, 0))

	// TODO: wait until earliest export time
	if err := WaitUntil(ctx, timeIsAfter(earliestStartTs), time.Second*30); err != nil {
		return fmt.Errorf("failed waiting for earliest export time: %w", err)
	}

	// TODO: always run consensus task
	walkCfg, err := walkForManifest(em)
	if err != nil {
		return fmt.Errorf("build walk for yesterday: %w", err)
	}
	ll.Infof("using tasks %s", strings.Join(walkCfg.Tasks, ","))

	api, closer, err := commands.GetAPI(ctx, lilyConfig.apiAddr, lilyConfig.apiToken)
	if err != nil {
		return fmt.Errorf("could not access lily api: %w", err)
	}
	defer closer()

	// TODO: don't start the walk if we have raw exports and they have no execution errors

	jobID, err := api.LilyWalk(ctx, walkCfg)
	if err != nil {
		return fmt.Errorf("failed to create walk: %w", err)
	}

	ll.Infof("started walk %s with id %d", walkCfg.Name, jobID)

	if err := WaitUntil(ctx, jobHasEnded(api, jobID), time.Second*30); err != nil {
		return fmt.Errorf("failed waiting for job to finish: %w", err)
	}

	jr, err := getJobResult(ctx, api, jobID)
	if err != nil {
		return fmt.Errorf("failed reading job result: %w", err)
	}

	if jr.Error != "" {
		// TODO: retry
		return fmt.Errorf("job failed with error: %s", jr.Error)
	}

	ll.Info("export complete")

	wi := WalkInfo{
		Name:   walkCfg.Name,
		Path:   storageConfig.path,
		Format: "csv",
	}

	// TODO: use processing report to look for execution errors
	_, err = verifyExport(ctx, em, wi, outputPath)
	if err != nil {
		return fmt.Errorf("failed to verify export files: %w", err)
	}

	err = shipExport(ctx, em, wi, outputPath, p)
	if err != nil {
		return fmt.Errorf("failed to ship export files: %w", err)
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

func getJobResult(ctx context.Context, api lily.LilyAPI, id schedule.JobID) (*schedule.JobResult, error) {
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
