package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/filecoin-project/sentinel-visor/lens/lily"
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
)

func init() {
	for _, table := range TableList {
		TablesByName[table.Name] = table
		for _, schema := range table.Schemas {
			TablesBySchema[schema] = append(TablesBySchema[schema], table)
		}
	}
}

type ExportManifest struct {
	Period ExportPeriod
	Files  []ExportFile
}

func manifestForDate(d Date, network string, genesisTs int64, outputPath string, schemaVersion int) (*ExportManifest, error) {
	p := firstExportPeriod(genesisTs)

	if p.Date.After(d) {
		return nil, fmt.Errorf("date is before genesis: %s", d.String())
	}

	// Iteration here guarantees we are always consistent with height ranges
	for p.Date != d {
		p = p.Next()
	}

	return manifestForPeriod(p, network, genesisTs, outputPath, schemaVersion)
}

func manifestForPeriod(p ExportPeriod, network string, genesisTs int64, outputPath string, schemaVersion int) (*ExportManifest, error) {
	em := &ExportManifest{
		Period: p,
	}

	for _, t := range TablesBySchema[schemaVersion] {
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
	walkName := fmt.Sprintf("exp%s-%d-%d", time.Now().UTC().Format("0102"), em.Period.StartHeight, em.Period.EndHeight)

	return &lily.LilyWalkConfig{
		Name:                walkName,
		Tasks:               tasksForManifest(em),
		Window:              300 * time.Second,
		From:                em.Period.StartHeight,
		To:                  em.Period.EndHeight,
		RestartDelay:        0,
		RestartOnCompletion: false,
		RestartOnFailure:    false,
		Storage:             storageConfig.name,
	}, nil
}
