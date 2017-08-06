package redis

import (
	"net"
	"testing"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/require"

	"github.com/chihaya/chihaya/bittorrent"
)

func getPeerStore() (redigo.Conn, *peerStore) {
	// ps, err := New(Config{
	// 	Host:      "localhost",
	// 	Port:      "6379",
	// 	KeyPrefix: "test:",
	// })
	// if err != nil {
	// 	panic(err)
	// }
	testConfig := Config{
		Host:                        "localhost",
		Port:                        "6379",
		KeyPrefix:                   "test:crud:",
		GarbageCollectionInterval:   1500000,
		PrometheusReportingInterval: 3000,
		MaxNumWant:                  10,
		MaxIdle:                     10,
		PeerLifetime:                15,
	}

	cfg := testConfig.Validate()
	server := cfg.Host + ":" + cfg.Port
	pool := redigo.Pool{
		MaxIdle:     cfg.MaxIdle,
		IdleTimeout: 30 * time.Second,
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
	conn := pool.Get()
	defer conn.Close()

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

	if cfg.Instance != 0 {
		conn.Do("SELECT", cfg.Instance)
	}

	return conn, ps
}

// func getPeerStore() (*redigomock.Conn, *peerStore) {
// 	conn := redigomock.NewConn()
// 	pool := redigo.Pool{
// 		MaxIdle: 10,
// 		Dial: func() (redigo.Conn, error) {
// 			return conn, nil
// 		},
// 		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
// 			_, err := c.Do("PING")
// 			return err
// 		},
// 	}
//
// 	return conn, &peerStore{
// 		connPool:     &pool,
// 		closed:       make(chan struct{}),
// 		maxNumWant:   3,
// 		peerLifetime: 15,
// 		gcValidity:   1500000,
// 	}
// }

var testData = []struct {
	vals      bittorrent.Peer
	ih        bittorrent.InfoHash
	numPeers  int
	peers     []bittorrent.Peer
	announcer bittorrent.Peer
	expected  error
	rangeVals []interface{}
	expectedL int64
}{
	{
		bittorrent.Peer{
			ID:   bittorrent.PeerIDFromString("12345678912345678912"),
			IP:   bittorrent.IP{IP: net.IP("112.71.10.240")},
			Port: 7002,
		},
		bittorrent.InfoHashFromString("12345678912345678912"),
		0,
		[]bittorrent.Peer{},
		bittorrent.Peer{},
		nil,
		[]interface{}{
			[]byte("12345678912345678912"),
			[]byte("abcdefgixxxxxxxxxxxxxxx"),
			[]byte("12354634ir78an0ob7151"),
			[]byte("000000000000000000000")},
		5,
	},
	{
		bittorrent.Peer{
			ID:   bittorrent.PeerIDFromString("#!@#$%^$*()&*#$*~al:"),
			IP:   bittorrent.IP{IP: net.IP("10:71:10:1A:2B")},
			Port: 1111,
		},
		bittorrent.InfoHashFromString("4:test3i:123:er123rt"),
		1,
		[]bittorrent.Peer{
			bittorrent.Peer{
				ID:   bittorrent.PeerIDFromString("totallydifferent1234"),
				IP:   bittorrent.IP{IP: net.IP("XX:71:10:1A:2X")},
				Port: 1234,
			},
		},
		bittorrent.Peer{},
		nil,
		[]interface{}{
			[]byte(""),
		},
		2,
	},
	{
		bittorrent.Peer{
			ID:   bittorrent.PeerIDFromString("////////////////////"),
			IP:   bittorrent.IP{IP: net.IP("192.168.0.2")},
			Port: 12356,
		},
		bittorrent.InfoHashFromString("////////////////////"),
		0,
		[]bittorrent.Peer{},
		bittorrent.Peer{},
		nil,
		[]interface{}{
			[]byte(""),
		},
		1,
	},
}

// func TestPeerStore(t *testing.T) {
// 	s.TestPeerStore(t, createNew())
// 	// TestAddPeer(t)
// 	// TestRemovePeers(t, createNew())
// 	// TestGetPeers(t, createNew())
// 	// TestGetSetLength(t, createNew())
// }

func TestAddPeer(t *testing.T) {
	_, ps := getPeerStore()
	// conn := ps.connPool.Get()
	// conn.Clear()
	for _, tp := range testData {
		// peer := fmt.Sprintf("%s%s", "seeder:", tp.ih)
		pk := newPeerKey(tp.vals)
		// conn.Command("MULTI").Expect("OK")
		// conn.Command("ZADD", peer, time.Now().Unix(), pk).Expect("QUEUED")
		// conn.Command("EXPIRE", peer, int(ps.peerLifetime.Seconds())).
		// 	Expect("QUEUED")
		// conn.Command("EXEC").Expect("1) OK\n2) OK")
		err := addPeer(ps, tp.ih, "seeder", pk)
		require.Equal(t, tp.expected, err, "addPeer redis fail:", err)
	}
}

func TestRemovePeers(t *testing.T) {
	_, ps := getPeerStore()
	// conn := ps.connPool.Get()
	// conn.Clear()
	for _, tp := range testData {
		// peer := fmt.Sprintf("%s%s", "seeder:", tp.ih)
		pk := newPeerKey(tp.vals)
		// conn.Command("ZREM", peer, pk).Expect("(integer) 1")
		err := removePeers(ps, tp.ih, "seeder", pk)
		require.Equal(t, tp.expected, err, "removePeers redis fail:", err)
	}
}

func TestGetPeers(t *testing.T) {
	_, ps := getPeerStore()
	// conn := ps.connPool.Get()
	// conn.Clear()
	for _, tp := range testData {
		// peer := fmt.Sprintf("%s%s", "leecher:", tp.ih)
		// conn.Command("ZREMRANGEBYSCORE", peer, "-inf",
		// 	fmt.Sprintf("%s%d", "(", time.Now().Add(-ps.peerLifetime).Unix())).
		// 	Expect("(integer 1)")
		// conn.Command("ZRANGE", peer, 0, -1).Expect(tp.rangeVals)
		peers, err := getPeers(ps, tp.ih, "leecher", tp.numPeers, tp.peers, tp.announcer)
		require.Equal(t, nil, err, "getPeers redis fail:", err)
		require.Equal(t, tp.numPeers, len(peers), "getPeers logic fail : peer length issue")
		for _, peerling := range peers {
			require.NotEqual(t, tp.announcer.ID, peerling.ID, "getPeers logic fail : announcer not ignored")
		}
	}
}

func TestGetPeersLength(t *testing.T) {
	_, ps := getPeerStore()
	// conn := ps.connPool.Get()
	// conn.Clear()
	for _, tp := range testData {
		// peer := fmt.Sprintf("%s%s", "leecher:", tp.ih)
		// conn.Command("ZCARD", peer).Expect(tp.expectedL)
		Actlen, err := getPeersLength(ps, tp.ih, "leecher")
		require.Equal(t, nil, err, "getPeersLength redis fail:", err)
		require.Equal(t, tp.expectedL, int64(Actlen), "getPeersLength logic fail: length mismatch")
	}
}

// func BenchmarkAddPeer(b *testing.B)        { s.addPeer(b, createNew()) }
// func BenchmarkRemovePeers(b *testing.B)    { s.removePeers(b, createNew()) }
// func BenchmarkGetPeers(b *testing.B)       { s.getPeers(b, createNew()) }
// func BenchmarkGetPeersLength(b *testing.B) { s.getPeersLength(b, createNew()) }
