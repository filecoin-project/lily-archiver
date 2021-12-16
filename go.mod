module github.com/iand/sentinel-archiver

go 1.16

require (
	github.com/filecoin-project/lily v0.8.2-0.20211022094029-438a4cac37cb
	github.com/filecoin-project/specs-actors/v5 v5.0.4
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-log/v2 v2.3.0
	github.com/urfave/cli/v2 v2.3.0
)

replace github.com/filecoin-project/filecoin-ffi => github.com/filecoin-project/ffi-stub v0.2.0

// See https://github.com/ipfs/go-mfs/pull/88
replace github.com/ipfs/go-mfs => github.com/ipfs/go-mfs v0.1.3-0.20210507195338-96fbfa122164
