package redis

import (
	"fmt"
	"time"

	redigo "github.com/garyburd/redigo/redis"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/pkg/log"
)

// Adds an expiry to the set, that self deletes if not refreshed
func addPeer(s *peerStore, infoHash bittorrent.InfoHash, peerType string, pk serializedPeer) error {
	conn := s.connPool.Get()
	defer conn.Close()
	Key := fmt.Sprintf("%s:%s", peerType, infoHash)
	conn.Send("MULTI")
	conn.Send("ZADD", Key, time.Now().Unix(), pk)
	conn.Send("EXPIRE", Key, int(s.peerLifetime.Seconds()))
	_, err := conn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func removePeers(s *peerStore, infoHash bittorrent.InfoHash, peerType string, pk serializedPeer) error {
	conn := s.connPool.Get()
	defer conn.Close()
	log.Info(fmt.Sprintf("Running test on <%+v:%+v>\n", peerType, infoHash))
	removed, err := conn.Do("ZREM", fmt.Sprintf("%s:%s", peerType, infoHash), pk)
	if err != nil {
		return err
	}
	// data := map[string]interface{}(removed)
	// data := map[int64]interface{}(removed)
	// data := make([]interface{}, len(removed))
	// for i, v := range removed {
	// 	data[i] = v
	// }
	// res, _ := json.Marshal(data)
	// log.Info(fmt.Sprintf("* Removed:\n%s\n", string(res)))
	// log.Info(fmt.Sprintf("* Removed:\n%+v\n", removed))
	log.Info(fmt.Sprintf("* Removed:\n%v, %T\n", removed, removed))
	// if removed == nil || removed == int64(0) {
	// 	// log.Info(fmt.Sprintf(">>> Got ya! \n%+v\n", removed))
	// 	return storage.ErrResourceDoesNotExist
	// }
	return nil
}

// Prunes the existing infohash swarm for any old peers before
// returning range of valid peers
func getPeers(s *peerStore, infoHash bittorrent.InfoHash, peerType string, numWant int, peers []bittorrent.Peer, excludePeers bittorrent.Peer) ([]bittorrent.Peer, error) {
	conn := s.connPool.Get()
	defer conn.Close()
	Key := fmt.Sprintf("%s:%s", peerType, infoHash)
	_, err := conn.Do("ZREMRANGEBYSCORE", Key, "-inf", fmt.Sprintf("(%d", time.Now().Add(-s.peerLifetime).Unix()))
	if err != nil {
		return nil, err
	}
	peerList, err := redigo.Strings(conn.Do("ZRANGE", Key, 0, -1))
	if err != nil {
		return nil, err
	}
	for _, p := range peerList {
		if numWant == len(peers) {
			break
		}
		pk := serializedPeer(p)
		decodedPeer := decodePeerKey(pk)
		if decodedPeer.Equal(excludePeers) {
			continue
		}
		peers = append(peers, decodedPeer)
	}
	return peers, nil
}

func getPeersLength(s *peerStore, infoHash bittorrent.InfoHash, peerType string) (int, error) {
	conn := s.connPool.Get()
	defer conn.Close()
	return redigo.Int(conn.Do("ZCARD", fmt.Sprintf("%s:%s", peerType, infoHash)))
}
