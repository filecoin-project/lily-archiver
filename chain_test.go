package main

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/network"
)

func TestNetworkVersionsBetweenHeights(t *testing.T) {
	simple := []NetworkHeight{
		{
			Version: 1,
			Height:  10,
		},
		{
			Version: 2,
			Height:  20,
		},
	}

	testCases := []struct {
		schedule []NetworkHeight
		from     abi.ChainEpoch
		to       abi.ChainEpoch
		want     []network.Version
	}{

		{
			schedule: simple,
			from:     1,
			to:       5,
			want:     []network.Version{0},
		},
		{
			schedule: simple,
			from:     11,
			to:       12,
			want:     []network.Version{1},
		},
		{
			schedule: simple,
			from:     21,
			to:       25,
			want:     []network.Version{2},
		},
		{
			schedule: simple,
			from:     1,
			to:       12,
			want:     []network.Version{0, 1},
		},
		{
			schedule: simple,
			from:     1,
			to:       100,
			want:     []network.Version{0, 1, 2},
		},
	}

	oldSchedule := UpgradeSchedule
	defer func() {
		UpgradeSchedule = oldSchedule
	}()

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d-%d", tc.from, tc.to), func(t *testing.T) {
			UpgradeSchedule = tc.schedule
			got := NetworkVersionsBetweenHeights(tc.from, tc.to)

			if len(got) != len(tc.want) {
				t.Errorf("got %+v, wanted %+v", got, tc.want)
				return
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("got %+v, wanted %+v", got, tc.want)
					return
				}
			}
		})
	}
}
