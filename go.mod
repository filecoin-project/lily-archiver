module github.com/iand/sentinel-archiver

go 1.16

require (
	github.com/filecoin-project/sentinel-visor v0.7.5
	github.com/filecoin-project/specs-actors/v5 v5.0.3
	github.com/ipfs/go-log/v2 v2.1.3
	github.com/urfave/cli/v2 v2.3.0
)

replace github.com/filecoin-project/filecoin-ffi => github.com/filecoin-project/ffi-stub v0.2.0
