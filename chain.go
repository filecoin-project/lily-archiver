package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/network"
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

type NetworkHeight struct {
	Version network.Version
	Height  abi.ChainEpoch
}

// UpgradeSchedule is a list of heights at which each network version starts, sorted by height ascending.
var UpgradeSchedule = []NetworkHeight{
	// {
	// 	Version: 0,
	// 	Height:  0,
	// },
}

func setUpgradeSchedule(s string) error {
	entries := strings.Split(s, ",")

	for _, entry := range entries {
		parts := strings.Split(entry, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid upgrade schedule entry %q, expected it to be in format \"{version}:{height}\"", entry)
		}

		version, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid upgrade schedule entry %q: %w", entry, err)
		}
		height, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid upgrade schedule entry %q: %w", entry, err)
		}

		UpgradeSchedule = append(UpgradeSchedule, NetworkHeight{
			Version: network.Version(version),
			Height:  abi.ChainEpoch(height),
		})
	}

	sort.Slice(UpgradeSchedule, func(a, b int) bool {
		return UpgradeSchedule[a].Height < UpgradeSchedule[b].Height
	})

	return nil
}

// NetworkVersionsBetweenHeights returns all network versions that were in use between the given heights
func NetworkVersionsBetweenHeights(from, to abi.ChainEpoch) []network.Version {
	if len(UpgradeSchedule) == 0 {
		return []network.Version{network.Version0}
	}

	// Shortcut to pick the last network version if the range is later than all upgrades
	last := UpgradeSchedule[len(UpgradeSchedule)-1]
	if from >= last.Height {
		return []network.Version{last.Version}
	}

	// Shortcut to pick the default network version if the range is before all upgrades
	first := UpgradeSchedule[0]
	if to < first.Height {
		return []network.Version{network.Version0}
	}

	lowestVersion := network.Version0
	highestVersion := network.Version0

	for _, nh := range UpgradeSchedule {
		if nh.Height <= from {
			lowestVersion = nh.Version
		}
		if nh.Height <= to {
			highestVersion = nh.Version
		}
		if nh.Height > to {
			break
		}
	}

	versions := []network.Version{}
	for v := lowestVersion; v <= highestVersion; v++ {
		versions = append(versions, network.Version(v))
	}

	return versions
}
