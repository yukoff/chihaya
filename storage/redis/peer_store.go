package redis

import (
	"encoding/binary"
	"net"
	"time"

	"gopkg.in/yaml.v2"

	redigo "github.com/garyburd/redigo/redis"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/pkg/log"
	"github.com/chihaya/chihaya/storage"
)

// Name is the name by which this peer store is registered with Chihaya.
const Name = "redis"

// Default config constants.
const (
	defaultPrometheusReportingInterval = time.Second * 1
	defaultGarbageCollectionInterval   = time.Minute * 3
	defaultKeyPrefix                   = ""
	defaultInstance                    = 0
	defaultMaxNumWant                  = 100
	defaultMaxIdle                     = 100
	defaultHost                        = "localhost"
	defaultPort                        = "6379"
	defaultPeerLifetime                = time.Minute * 30
)

func init() {
	// Register the storage driver.
	storage.RegisterDriver(Name, driver{})
}

type driver struct{}

func (d driver) NewPeerStore(icfg interface{}) (storage.PeerStore, error) {
	// Marshal the config back into bytes.
	bytes, err := yaml.Marshal(icfg)
	if err != nil {
		return nil, err
	}

	// Unmarshal the bytes into the proper config type.
	var cfg Config
	err = yaml.Unmarshal(bytes, &cfg)
	if err != nil {
		return nil, err
	}

	return New(cfg)
}

const (
	ipv4 = "4"
	ipv6 = "6"
)

// Config holds the configuration of a redis peerstore.
// KeyPrefix specifies the prefix that could optionally precede keys
// Instance specifies the redis database number to connect to (default 0)
// MaxNumWant is the maximum number of peers to return to announce
type Config struct {
	GarbageCollectionInterval   time.Duration `yaml:"gc_interval"`
	PrometheusReportingInterval time.Duration `yaml:"prometheus_reporting_interval"`
	KeyPrefix                   string        `yaml:"key_prefix"`
	Instance                    int           `yaml:"instance"`
	MaxNumWant                  int           `yaml:"max_numwant"`
	MaxIdle                     int           `yaml:"max_idle"`
	Host                        string        `yaml:"host"`
	Port                        string        `yaml:"port"`
	Password                    string        `yaml:"password"`
	PeerLifetime                time.Duration `yaml:"peer_liftetime"`
}

// LogFields renders the current config as a set of Logrus fields.
func (cfg Config) LogFields() log.Fields {
	return log.Fields{
		"name":               Name,
		"gcInterval":         cfg.GarbageCollectionInterval,
		"promReportInterval": cfg.PrometheusReportingInterval,
		"keyPrefix":          cfg.KeyPrefix,
		"instance":           cfg.Instance,
		"maxNumWant":         cfg.MaxNumWant,
		"maxIdle":            cfg.MaxIdle,
		"host":               cfg.Host,
		"port":               cfg.Port,
		"peerLifetime":       cfg.PeerLifetime,
	}
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() Config {
	validcfg := cfg

	if cfg.GarbageCollectionInterval <= 0 {
		validcfg.GarbageCollectionInterval = defaultGarbageCollectionInterval
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".GarbageCollectionInterval",
			"provided": cfg.GarbageCollectionInterval,
			"default":  validcfg.GarbageCollectionInterval,
		})
	}

	if cfg.PrometheusReportingInterval <= 0 {
		validcfg.PrometheusReportingInterval = defaultPrometheusReportingInterval
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".PrometheusReportingInterval",
			"provided": cfg.PrometheusReportingInterval,
			"default":  validcfg.PrometheusReportingInterval,
		})
	}

	if cfg.Instance < 0 {
		validcfg.Instance = defaultInstance
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".Instance",
			"provided": cfg.Instance,
			"default":  validcfg.Instance,
		})
	}

	if cfg.MaxNumWant <= 0 {
		validcfg.MaxNumWant = defaultMaxNumWant
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".MaxNumWant",
			"provided": cfg.MaxNumWant,
			"default":  validcfg.MaxNumWant,
		})
	}

	if cfg.MaxIdle <= 0 {
		validcfg.MaxIdle = defaultMaxIdle
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".MaxIdle",
			"provided": cfg.MaxIdle,
			"default":  validcfg.MaxIdle,
		})
	}

	if cfg.Host == "" {
		validcfg.Host = defaultHost
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".Host",
			"provided": cfg.Host,
			"default":  validcfg.Host,
		})
	}

	if cfg.Port == "" {
		validcfg.Port = defaultPort
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".Port",
			"provided": cfg.Port,
			"default":  validcfg.Port,
		})
	}

	if cfg.PeerLifetime <= 0 {
		validcfg.PeerLifetime = defaultPeerLifetime
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".PeerLifetime",
			"provided": cfg.PeerLifetime,
			"default":  validcfg.PeerLifetime,
		})
	}

	return validcfg
}

func newPool(server string, maxIdle int) redigo.Pool {
	return redigo.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// New creates a new PeerStore backed by redis.
func New(provided Config) (storage.PeerStore, error) {
	cfg := provided.Validate()
	pool := newPool(cfg.Host+":"+cfg.Port, cfg.MaxIdle)
	conn := pool.Get()
	defer conn.Close()

	if cfg.Instance != 0 {
		conn.Do("SELECT", cfg.Instance)
	}

	ps := &peerStore{
		cfg:              cfg,
		swarms:           make(map[bittorrent.InfoHash]swarm),
		connPool:         &pool,
		closed:           make(chan struct{}),
		maxNumWant:       cfg.MaxNumWant,
		maxIdle:          cfg.MaxIdle,
		peerLifetime:     cfg.PeerLifetime,
		seederKeyPrefix:  cfg.KeyPrefix + "seeder",
		leecherKeyPrefix: cfg.KeyPrefix + "leecher",
	}

	// Start a goroutine for garbage collection.

	// Start a goroutine for updating our cached system clock.

	// Start a goroutine for reporting statistics to Prometheus.

	return ps, nil
}

type serializedPeer string

type swarm struct {
	// map serialized peer to mtime
	seeders  map[serializedPeer]int64
	leechers map[serializedPeer]int64
}

type peerStore struct {
	// According to the sync/atomic docs, "The first word in a global variable or
	// in an allocated struct or slice can be relied upon to be 64-bit aligned."
	// Adding the 4 byte padding below does the trick.
	_                [4]byte
	cfg              Config
	swarms           map[bittorrent.InfoHash]swarm
	connPool         *redigo.Pool
	closed           chan struct{}
	maxNumWant       int
	maxIdle          int
	peerLifetime     time.Duration
	gcValidity       int
	seederKeyPrefix  string
	leecherKeyPrefix string

	// clock stores the current time nanoseconds, updated every second.
	// Must be accessed atomically!
	clock int64
	// wg    sync.WaitGroup
}

var _ storage.PeerStore = &peerStore{}

func newPeerKey(p bittorrent.Peer) serializedPeer {
	b := make([]byte, 20+2+len(p.IP.IP))
	copy(b[:20], p.ID[:])
	binary.BigEndian.PutUint16(b[20:22], p.Port)
	copy(b[22:], p.IP.IP)

	return serializedPeer(b)
}

func decodePeerKey(pk serializedPeer) bittorrent.Peer {
	peer := bittorrent.Peer{
		ID:   bittorrent.PeerIDFromString(string(pk[:20])),
		Port: binary.BigEndian.Uint16([]byte(pk[20:22])),
		IP:   bittorrent.IP{IP: net.IP(pk[22:])}}

	if ip := peer.IP.To4(); ip != nil {
		peer.IP.IP = ip
		peer.IP.AddressFamily = bittorrent.IPv4
	} else if len(peer.IP.IP) == net.IPv6len { // implies toReturn.IP.To4() == nil
		peer.IP.AddressFamily = bittorrent.IPv6
	} else {
		panic("IP is neither v4 nor v6")
	}

	return peer
}

func panicIfClosed(closed <-chan struct{}) {
	select {
	case <-closed:
		panic("attempted to interact with stopped redis store")
	default:
	}
}

func ipType(ip net.IP) string {
	if len(ip) == net.IPv6len {
		return ipv6
	}
	return ipv4
}

func (ps *peerStore) PutSeeder(infoHash bittorrent.InfoHash, p bittorrent.Peer) error {
	panicIfClosed(ps.closed)

	pk := newPeerKey(p)
	return addPeer(ps, infoHash, ps.seederKeyPrefix+ipType(p.IP.IP), pk)
}

func (ps *peerStore) DeleteSeeder(infoHash bittorrent.InfoHash, p bittorrent.Peer) error {
	panicIfClosed(ps.closed)
	pk := newPeerKey(p)
	return removePeers(ps, infoHash, ps.seederKeyPrefix+ipType(p.IP.IP), pk)
}

func (ps *peerStore) PutLeecher(infoHash bittorrent.InfoHash, p bittorrent.Peer) error {
	panicIfClosed(ps.closed)
	pk := newPeerKey(p)
	return addPeer(ps, infoHash, ps.leecherKeyPrefix+ipType(p.IP.IP), pk)
}

func (ps *peerStore) DeleteLeecher(infoHash bittorrent.InfoHash, p bittorrent.Peer) error {
	panicIfClosed(ps.closed)
	pk := newPeerKey(p)
	return removePeers(ps, infoHash, ps.leecherKeyPrefix+ipType(p.IP.IP), pk)
}

func (ps *peerStore) GraduateLeecher(infoHash bittorrent.InfoHash, p bittorrent.Peer) error {
	panicIfClosed(ps.closed)
	err := ps.PutSeeder(infoHash, p)
	if err != nil {
		return err
	}
	err = ps.DeleteLeecher(infoHash, p)
	if err != nil {
		return err
	}
	return nil
}

// Announce as many peers as possible based on the announcer being
// a seeder or leecher
func (ps *peerStore) AnnouncePeers(infoHash bittorrent.InfoHash, seeder bool, numWant int, announcer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	panicIfClosed(ps.closed)
	if numWant > ps.maxNumWant {
		numWant = ps.maxNumWant
	}

	if seeder {
		peers, err = getPeers(ps, infoHash, ps.leecherKeyPrefix+ipType(announcer.IP.IP), numWant, peers, bittorrent.Peer{})
		if err != nil {
			return nil, err
		}
	} else {
		peers, err = getPeers(ps, infoHash, ps.seederKeyPrefix+ipType(announcer.IP.IP), numWant, peers, bittorrent.Peer{})
		if err != nil {
			return nil, err
		}
		if len(peers) < numWant {
			peers, err = getPeers(ps, infoHash, ps.leecherKeyPrefix+ipType(announcer.IP.IP), numWant, peers, announcer)
		}
	}
	return peers, nil
}

func (ps *peerStore) ScrapeSwarm(infoHash bittorrent.InfoHash, addressFamily bittorrent.AddressFamily) (resp bittorrent.Scrape) {
	panicIfClosed(ps.closed)

	ipType := ipv4
	if addressFamily == bittorrent.IPv6 {
		ipType = ipv6
	}

	complete, err := getPeersLength(ps, infoHash, ps.seederKeyPrefix+ipType)
	if err != nil {
		return
	}
	resp.Complete = uint32(complete)
	incomplete, err := getPeersLength(ps, infoHash, ps.leecherKeyPrefix+ipType)
	if err != nil {
		return
	}
	resp.Incomplete = uint32(incomplete)
	return
}

func (ps *peerStore) Stop() <-chan error {
	toReturn := make(chan error)
	go func() {
		close(ps.closed)
		close(toReturn)
	}()
	return toReturn
}

func (ps *peerStore) LogFields() log.Fields {
	return ps.cfg.LogFields()
}
