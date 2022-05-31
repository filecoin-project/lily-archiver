package main

import (
	"fmt"
	"strings"

	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/lily/chain/indexer/tasktype"
	"github.com/filecoin-project/lily/model/actors/common"
	init_ "github.com/filecoin-project/lily/model/actors/init"
	"github.com/filecoin-project/lily/model/actors/market"
	"github.com/filecoin-project/lily/model/actors/miner"
	"github.com/filecoin-project/lily/model/actors/multisig"
	"github.com/filecoin-project/lily/model/actors/power"
	"github.com/filecoin-project/lily/model/actors/reward"
	"github.com/filecoin-project/lily/model/actors/verifreg"
	"github.com/filecoin-project/lily/model/blocks"
	"github.com/filecoin-project/lily/model/chain"
	"github.com/filecoin-project/lily/model/derived"
	"github.com/filecoin-project/lily/model/messages"
	"github.com/filecoin-project/lily/model/msapprovals"
	"github.com/go-pg/pg/v10/orm"
)

type Table struct {
	// Name is the name of the table
	Name string

	// Task is the name of the task that writes the table
	Task string

	// Schema is the major schema version for which the table is supported.
	Schema int

	// NetworkVersionRange is the range filecoin network versions for which the table is supported.
	NetworkVersionRange NetworkVersionRange

	// An empty instance of the lily model
	Model interface{}
}

type NetworkVersionRange struct {
	From network.Version
	To   network.Version
}

var AllNetWorkVersions = NetworkVersionRange{From: network.Version0, To: network.VersionMax}

var TableList = []Table{
	{
		Name:                "actor_states",
		Schema:              1,
		Task:                tasktype.ActorState,
		Model:               &common.ActorState{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "actors",
		Schema:              1,
		Task:                tasktype.Actor,
		Model:               &common.Actor{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "block_headers",
		Schema:              1,
		Task:                tasktype.BlockHeader,
		Model:               &blocks.BlockHeader{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "block_messages",
		Schema:              1,
		Task:                tasktype.BlockMessage,
		Model:               &messages.BlockMessage{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "block_parents",
		Schema:              1,
		Task:                tasktype.BlockParent,
		Model:               &blocks.BlockParent{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "chain_consensus",
		Schema:              1,
		Task:                tasktype.ChainConsensus,
		Model:               &chain.ChainConsensus{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "chain_economics",
		Schema:              1,
		Task:                tasktype.ChainEconomics,
		Model:               &chain.ChainEconomics{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "chain_powers",
		Schema:              1,
		Task:                tasktype.ChainPower,
		Model:               &power.ChainPower{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "chain_rewards",
		Schema:              1,
		Task:                tasktype.ChainReward,
		Model:               &reward.ChainReward{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "derived_gas_outputs",
		Schema:              1,
		Task:                tasktype.GasOutputs,
		Model:               &derived.GasOutputs{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "drand_block_entries",
		Schema:              1,
		Task:                tasktype.DrandBlockEntrie,
		Model:               &blocks.DrandBlockEntrie{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "id_addresses",
		Schema:              1,
		Task:                tasktype.IdAddress,
		Model:               &init_.IdAddress{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "internal_messages",
		Schema:              1,
		Task:                tasktype.InternalMessage,
		Model:               &messages.InternalMessage{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "internal_parsed_messages",
		Schema:              1,
		Task:                tasktype.InternalParsedMessage,
		Model:               &messages.InternalParsedMessage{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "market_deal_proposals",
		Schema:              1,
		Task:                tasktype.MarketDealProposal,
		Model:               &market.MarketDealProposal{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "market_deal_states",
		Schema:              1,
		Task:                tasktype.MarketDealState,
		Model:               &market.MarketDealState{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "message_gas_economy",
		Schema:              1,
		Task:                tasktype.MessageGasEconomy,
		Model:               &messages.MessageGasEconomy{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "messages",
		Schema:              1,
		Task:                tasktype.Message,
		Model:               &messages.Message{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "miner_current_deadline_infos",
		Schema:              1,
		Task:                tasktype.MinerCurrentDeadlineInfo,
		Model:               &miner.MinerCurrentDeadlineInfo{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "miner_fee_debts",
		Schema:              1,
		Task:                tasktype.MinerFeeDebt,
		Model:               &miner.MinerFeeDebt{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "miner_infos",
		Schema:              1,
		Task:                tasktype.MinerInfo,
		Model:               &miner.MinerInfo{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "miner_locked_funds",
		Schema:              1,
		Task:                tasktype.MinerLockedFund,
		Model:               &miner.MinerLockedFund{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "miner_pre_commit_infos",
		Schema:              1,
		Task:                tasktype.MinerPreCommitInfo,
		Model:               &miner.MinerPreCommitInfo{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "miner_sector_deals",
		Schema:              1,
		Task:                tasktype.MinerSectorDeal,
		Model:               &miner.MinerSectorDeal{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "miner_sector_events",
		Schema:              1,
		Task:                tasktype.MinerSectorEvent,
		Model:               &miner.MinerSectorEvent{},
		NetworkVersionRange: AllNetWorkVersions,
	},

	// added for actors v7 in network v15
	{
		Name:                "miner_sector_infos_v7",
		Schema:              1,
		Task:                tasktype.MinerSectorInfoV7,
		Model:               &miner.MinerSectorInfoV7{},
		NetworkVersionRange: NetworkVersionRange{From: network.Version15, To: network.VersionMax},
	},

	// used for actors v6 and below, up to network v14
	{
		Name:                "miner_sector_infos",
		Schema:              1,
		Task:                tasktype.MinerSectorInfoV1_6,
		Model:               &miner.MinerSectorInfoV1_6{},
		NetworkVersionRange: NetworkVersionRange{From: network.Version0, To: network.Version14},
	},
	{
		Name:                "miner_sector_posts",
		Schema:              1,
		Task:                tasktype.MinerSectorPost,
		Model:               &miner.MinerSectorPost{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "multisig_approvals",
		Schema:              1,
		Task:                tasktype.MultisigApproval,
		Model:               &msapprovals.MultisigApproval{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "multisig_transactions",
		Schema:              1,
		Task:                tasktype.MultisigTransaction,
		Model:               &multisig.MultisigTransaction{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "parsed_messages",
		Schema:              1,
		Task:                tasktype.ParsedMessage,
		Model:               &messages.ParsedMessage{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "power_actor_claims",
		Schema:              1,
		Task:                tasktype.PowerActorClaim,
		Model:               &power.PowerActorClaim{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "receipts",
		Schema:              1,
		Task:                tasktype.Receipt,
		Model:               &messages.Receipt{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "verified_registry_verifiers",
		Schema:              1,
		Task:                tasktype.VerifiedRegistryVerifier,
		Model:               &verifreg.VerifiedRegistryVerifier{},
		NetworkVersionRange: AllNetWorkVersions,
	},
	{
		Name:                "verified_registry_verified_clients",
		Schema:              1,
		Task:                tasktype.VerifiedRegistryVerifiedClient,
		Model:               &verifreg.VerifiedRegistryVerifiedClient{},
		NetworkVersionRange: AllNetWorkVersions,
	},
}

var (
	// TablesByName maps a table name to the table description.
	TablesByName = map[string]Table{}

	// KnownTasks is a lookup of known task names
	KnownTasks = map[string]struct{}{}

	// TablesBySchema maps a schema version to a list of tables present in that schema.
	TablesBySchema = map[int][]Table{}
)

func init() {
	for _, table := range TableList {
		TablesByName[table.Name] = table
		KnownTasks[table.Task] = struct{}{}
		TablesBySchema[table.Schema] = append(TablesBySchema[table.Schema], table)
	}
}

func TablesByTask(task string, schemaVersion int) []Table {
	tables := []Table{}
	for _, table := range TableList {
		if table.Task == task {
			tables = append(tables, table)
		}
	}
	return tables
}

func TableHeaders(v interface{}) ([]string, error) {
	q := orm.NewQuery(nil, v)
	tm := q.TableModel()
	m := tm.Table()

	if len(m.Fields) == 0 {
		return nil, fmt.Errorf("invalid table model: no fields found")
	}

	var columns []string

	for _, fld := range m.Fields {
		columns = append(columns, fld.SQLName)
	}
	return columns, nil
}

func TableSchema(v interface{}) (string, error) {
	q := orm.NewQuery(nil, v)
	tm := q.TableModel()
	m := tm.Table()

	if len(m.Fields) == 0 {
		return "", fmt.Errorf("invalid table model: no fields found")
	}

	name := strings.Trim(string(m.SQLNameForSelects), `"`)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("create table %s (\n", name))

	var fieldDefs []string
	for _, fld := range m.Fields {
		fieldDefs = append(fieldDefs, fmt.Sprintf("  %s %s", fld.Column, fld.SQLType))
	}
	sb.WriteString(strings.Join(fieldDefs, ",\n"))
	sb.WriteString("\n);\n")

	return sb.String(), nil
}
