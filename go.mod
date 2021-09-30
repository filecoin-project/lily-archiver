module github.com/iand/sentinel-archiver

go 1.16

require (
	github.com/filecoin-project/sentinel-visor v0.7.5
	github.com/filecoin-project/specs-actors/v5 v5.0.3
	github.com/ipfs/go-bitswap v0.3.4
	github.com/ipfs/go-blockservice v0.1.4
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ds-badger v0.2.6
	github.com/ipfs/go-filestore v1.0.0
	github.com/ipfs/go-ipfs-api v0.2.0
	github.com/ipfs/go-ipfs-blockstore v1.0.3
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipfs-provider v0.5.1
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-ipns v0.0.2
	github.com/ipfs/go-log/v2 v2.1.3
	github.com/ipfs/go-merkledag v0.3.2
	github.com/ipfs/go-mfs v0.1.2
	github.com/ipfs/go-unixfs v0.2.4
	github.com/libp2p/go-libp2p v0.14.2
	github.com/libp2p/go-libp2p-connmgr v0.2.4
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-kad-dht v0.11.0
	github.com/libp2p/go-libp2p-record v0.1.3
	github.com/libp2p/go-libp2p-tls v0.1.3
	github.com/multiformats/go-multiaddr v0.3.1
	github.com/multiformats/go-multihash v0.0.15
	github.com/urfave/cli/v2 v2.3.0
)

replace github.com/filecoin-project/filecoin-ffi => github.com/filecoin-project/ffi-stub v0.2.0
