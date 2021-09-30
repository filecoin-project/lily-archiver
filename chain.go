package main

import (
	"time"

	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
)

const (
	MainnetGenesisTs = 1598306400 // unix timestamp of genesis epoch
	Finality         = int64(miner.ChainFinality)
	EpochsInDay      = builtin.EpochsInDay
)

// HeightToUnix converts a chain height to a unix timestamp given the unix timestamp of the genesis epoch.
func HeightToUnix(height int64, genesisTs int64) int64 {
	return height*builtin.EpochDurationSeconds + genesisTs
}

// UnixToHeight converts a unix timestamp a chain height given the unix timestamp of the genesis epoch.
func UnixToHeight(ts int64, genesisTs int64) int64 {
	return (ts - genesisTs) / builtin.EpochDurationSeconds
}

// CurrentHeight calculates the current height of the filecoin mainnet.
func CurrentHeight(genesisTs int64) int64 {
	return UnixToHeight(time.Now().Unix(), genesisTs)
}
