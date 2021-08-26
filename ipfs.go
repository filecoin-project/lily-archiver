package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ipfs/go-bitswap"
	"github.com/ipfs/go-bitswap/network"
	blockservice "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ds-badger"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	provider "github.com/ipfs/go-ipfs-provider"
	"github.com/ipfs/go-ipfs-provider/queue"
	"github.com/ipfs/go-ipfs-provider/simple"
	ipld "github.com/ipfs/go-ipld-format"
	ipns "github.com/ipfs/go-ipns"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	"github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	host "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	routing "github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	dualdht "github.com/libp2p/go-libp2p-kad-dht/dual"
	record "github.com/libp2p/go-libp2p-record"
	libp2ptls "github.com/libp2p/go-libp2p-tls"
	"github.com/multiformats/go-multiaddr"
	multihash "github.com/multiformats/go-multihash"
)

var BootstrapPeers = []peer.AddrInfo{
	mustParseAddr("/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"),
	mustParseAddr("/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa"),
	mustParseAddr("/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"),
	mustParseAddr("/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt"),
}

func mustParseAddr(addr string) peer.AddrInfo {
	ma, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		panic(fmt.Sprintf("failed to parse bootstrap address: %v", err))
	}

	ai, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		panic(fmt.Sprintf("failed to create address info: %v", err))
	}

	return *ai
}

var defaultReprovideInterval = 12 * time.Hour

// Config wraps configuration options for the Peer.
type PeerConfig struct {
	Offline           bool
	ReprovideInterval time.Duration
	DatastorePath     string
	ListenAddr        string
	Libp2pKeyFile     string
}

type Peer struct {
	mu sync.Mutex

	offline           bool
	reprovideInterval time.Duration
	listenAddr        multiaddr.Multiaddr
	peerKey           crypto.PrivKey
	datastorePath     string

	host  host.Host
	dht   routing.Routing
	store datastore.Batching

	ipld.DAGService // become a DAG service
	bstore          blockstore.Blockstore
	bserv           blockservice.BlockService
	reprovider      provider.System
}

func NewPeer(cfg *PeerConfig) (*Peer, error) {
	p := new(Peer)
	if err := p.applyConfig(cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	if err := p.setupDatastore(); err != nil {
		return nil, fmt.Errorf("setup datastore: %w", err)
	}

	if err := p.setupBlockstore(); err != nil {
		return nil, fmt.Errorf("setup blockstore: %w", err)
	}

	if !p.offline {
		if err := p.setupLibp2p(); err != nil {
			return nil, fmt.Errorf("setup libp2p: %w", err)
		}
		if err := p.bootstrap(BootstrapPeers); err != nil {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	if err := p.setupBlockService(); err != nil {
		return nil, fmt.Errorf("setup blockservice: %w", err)
	}

	if err := p.setupDAGService(); err != nil {
		p.bserv.Close()
		return nil, fmt.Errorf("setup dagservice: %w", err)
	}

	return p, nil
}

func (p *Peer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reprovider != nil {
		p.reprovider.Close()
		p.reprovider = nil
	}
	if p.bserv != nil {
		p.bserv.Close()
		p.bserv = nil
	}

	return nil
}

func (p *Peer) applyConfig(cfg *PeerConfig) error {
	if cfg == nil {
		cfg = &PeerConfig{}
	}

	p.offline = cfg.Offline
	if cfg.ReprovideInterval == 0 {
		p.reprovideInterval = defaultReprovideInterval
	} else {
		p.reprovideInterval = cfg.ReprovideInterval
	}

	if cfg.DatastorePath == "" {
		return fmt.Errorf("missing datastore path")
	}
	p.datastorePath = cfg.DatastorePath

	var err error
	p.listenAddr, err = multiaddr.NewMultiaddr(cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen addr: %w", err)
	}

	if cfg.Libp2pKeyFile == "" {
		return fmt.Errorf("missing libp2p keyfile")
	}
	p.peerKey, err = loadOrInitPeerKey(cfg.Libp2pKeyFile)
	if err != nil {
		return fmt.Errorf("key file: %w", err)
	}

	return nil
}

func (p *Peer) setupDatastore() error {
	logger.Infof("setting up ipfs datastore at %s", p.datastorePath)
	opts := badger.DefaultOptions

	ds, err := badger.NewDatastore(p.datastorePath, &opts)
	if err != nil {
		return fmt.Errorf("new datastore: %w", err)
	}
	p.store = ds
	return nil
}

func (p *Peer) setupBlockstore() error {
	logger.Info("setting up ipfs blockstore")
	bs := blockstore.NewBlockstore(p.store)
	bs = blockstore.NewIdStore(bs)
	cachedbs, err := blockstore.CachedBlockstore(context.TODO(), bs, blockstore.DefaultCacheOpts())
	if err != nil {
		return fmt.Errorf("new cached blockstore: %w", err)
	}
	p.bstore = cachedbs
	return nil
}

func (p *Peer) setupBlockService() error {
	logger.Info("setting up ipfs block service")
	if p.offline {
		p.bserv = blockservice.New(p.bstore, offline.Exchange(p.bstore))
		return nil
	}

	bswapnet := network.NewFromIpfsHost(p.host, p.dht)
	bswap := bitswap.New(context.TODO(), bswapnet, p.bstore)
	p.bserv = blockservice.New(p.bstore, bswap)
	return nil
}

func (p *Peer) setupDAGService() error {
	p.DAGService = merkledag.NewDAGService(p.bserv)
	return nil
}

func (p *Peer) setupReprovider() error {
	logger.Info("setting up reprovider")
	if p.offline || p.reprovideInterval < 0 {
		p.reprovider = provider.NewOfflineProvider()
		return nil
	}

	queue, err := queue.NewQueue(context.TODO(), "repro", p.store)
	if err != nil {
		return err
	}

	prov := simple.NewProvider(
		context.TODO(),
		queue,
		p.dht,
	)

	reprov := simple.NewReprovider(
		context.TODO(),
		p.reprovideInterval,
		p.dht,
		simple.NewBlockstoreProvider(p.bstore),
	)

	p.reprovider = provider.NewSystem(prov, reprov)
	p.reprovider.Run()
	return nil
}

func (p *Peer) bootstrap(peers []peer.AddrInfo) error {
	logger.Info("bootstrapping ipfs node")
	connected := make(chan struct{}, len(peers))

	var wg sync.WaitGroup
	for _, pinfo := range peers {
		// h.Peerstore().AddAddrs(pinfo.ID, pinfo.Addrs, peerstore.PermanentAddrTTL)
		wg.Add(1)
		go func(pinfo peer.AddrInfo) {
			defer wg.Done()
			err := p.host.Connect(context.TODO(), pinfo)
			if err != nil {
				logger.Warn(err)
				return
			}
			logger.Debugf("Connected to %s", pinfo.ID)
			connected <- struct{}{}
		}(pinfo)
	}

	wg.Wait()
	close(connected)

	i := 0
	for range connected {
		i++
	}

	logger.Debugf("connected to %d peers", len(peers))

	err := p.dht.Bootstrap(context.TODO())
	if err != nil {
		return fmt.Errorf("dht bootstrap: %w", err)
	}

	return nil
}

func (p *Peer) setupLibp2p() error {
	var ddht *dualdht.DHT
	var err error

	finalOpts := []libp2p.Option{
		libp2p.Identity(p.peerKey),
		libp2p.ListenAddrs(p.listenAddr),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			ddht, err = newDHT(context.TODO(), h, p.store)
			return ddht, err
		}),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connmgr.NewConnManager(100, 600, time.Minute)),
		libp2p.EnableAutoRelay(),
		libp2p.EnableNATService(),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
		libp2p.DefaultTransports,
	}

	h, err := libp2p.New(
		context.TODO(),
		finalOpts...,
	)
	if err != nil {
		return fmt.Errorf("libp2p: %w", err)
	}

	p.host = h
	p.dht = ddht

	return nil
}

func (p *Peer) AddFile(ctx context.Context, r io.Reader) (ipld.Node, error) {
	const hashfunc = "sha2-256"

	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, fmt.Errorf("bad CID Version: %s", err)
	}

	hashFunCode, ok := multihash.Names[hashfunc]
	if !ok {
		return nil, fmt.Errorf("unrecognized hash function: %s", hashfunc)
	}
	prefix.MhType = hashFunCode
	prefix.MhLength = -1

	dbp := helpers.DagBuilderParams{
		Dagserv:    p,
		RawLeaves:  true,
		Maxlinks:   helpers.DefaultLinksPerBlock,
		NoCopy:     false,
		CidBuilder: &prefix,
	}

	chnk := chunk.NewRabin(r, 1<<13)
	dbh, err := dbp.New(chnk)
	if err != nil {
		return nil, fmt.Errorf("new dag builder helper: %w", err)
	}

	return balanced.Layout(dbh)
}

func newDHT(ctx context.Context, h host.Host, ds datastore.Batching) (*dualdht.DHT, error) {
	dhtOpts := []dualdht.Option{
		dualdht.DHTOption(dht.NamespacedValidator("pk", record.PublicKeyValidator{})),
		dualdht.DHTOption(dht.NamespacedValidator("ipns", ipns.Validator{KeyBook: h.Peerstore()})),
		dualdht.DHTOption(dht.Concurrency(10)),
		dualdht.DHTOption(dht.Mode(dht.ModeAuto)),
	}
	if ds != nil {
		dhtOpts = append(dhtOpts, dualdht.DHTOption(dht.Datastore(ds)))
	}

	return dualdht.New(ctx, h, dhtOpts...)
}

func loadOrInitPeerKey(kf string) (crypto.PrivKey, error) {
	data, err := ioutil.ReadFile(kf)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		keyDir := filepath.Dir(kf)
		if err := os.MkdirAll(keyDir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("mkdir %q: %w", keyDir, err)
		}

		k, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}

		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(kf, data, 0o600); err != nil {
			return nil, err
		}

		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
}
