package iavl

import (
	"bytes"
	"fmt"
	"time"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/gogo/protobuf/proto"
	lru "github.com/hashicorp/golang-lru/v2"
)

type NodeBackend interface {
	// QueueNode queues a node for storage.
	QueueNode(*Node) error

	// QueueOrphan queues a node for orphaning.
	QueueOrphan(*Node) error

	// Commit commits all queued nodes to storage.
	Commit(int64) error

	GetNode(nodeKey []byte) (*Node, error)
}

type NodeCache interface {
	Add(nodeCacheKey, *Node)
	Remove(nodeCacheKey)
	Size() int
	Get(nodeCacheKey) (*Node, bool)
}

var _ NodeCache = (*NodeLruCache)(nil)

type NodeLruCache struct {
	cache *lru.Cache[nodeCacheKey, *Node]
}

func (c *NodeLruCache) Add(nk nodeCacheKey, node *Node) {
	c.cache.Add(nk, node)
}

func (c *NodeLruCache) Get(nk nodeCacheKey) (*Node, bool) {
	return c.cache.Get(nk)
}

func (c *NodeLruCache) Remove(nk nodeCacheKey) {
	c.cache.Remove(nk)
}

func (c *NodeLruCache) Size() int {
	return c.cache.Len()
}

func NewNodeLruCache(size int) *NodeLruCache {
	cache, err := lru.New[nodeCacheKey, *Node](size)
	if err != nil {
		panic(err)
	}
	return &NodeLruCache{
		cache: cache,
	}
}

var _ NodeCache = (*NodeMapCache)(nil)

type NodeMapCache struct {
	cache map[nodeCacheKey]*Node
}

func (c *NodeMapCache) Add(nk nodeCacheKey, node *Node) {
	c.cache[nk] = node
}

func (c *NodeMapCache) Get(nk nodeCacheKey) (*Node, bool) {
	node, ok := c.cache[nk]
	return node, ok
}

func (c *NodeMapCache) Remove(nk nodeCacheKey) {
	delete(c.cache, nk)
}

func (c *NodeMapCache) Size() int {
	return len(c.cache)
}

var _ NodeBackend = (*MapDB)(nil)

type MapDB struct {
	nodes map[[12]byte]*Node
	add   []*Node
	del   []*Node

	// simulation
	walBuf *bytes.Buffer
}

func NewMapDB() *MapDB {
	return &MapDB{
		nodes: make(map[[12]byte]*Node),
	}
}

func (m *MapDB) QueueNode(node *Node) error {
	// var nk [12]byte
	// copy(nk[:], node.nodeKey.GetKey())
	// m.nodes[nk] = node
	m.add = append(m.add, node)
	return nil
}

func (m *MapDB) QueueOrphan(node *Node) error {
	// var nk [12]byte
	// copy(nk[:], node.nodeKey.GetKey())
	// delete(m.nodes, nk)
	m.del = append(m.del, node)
	return nil
}

func (m *MapDB) Commit(version int64) error {
	for _, node := range m.add {
		var nk [12]byte
		copy(nk[:], node.nodeKey.GetKey())
		m.nodes[nk] = node
	}
	for _, node := range m.del {
		var nk [12]byte
		copy(nk[:], node.nodeKey.GetKey())
		delete(m.nodes, nk)
	}
	m.add = nil
	m.del = nil
	return nil
}

func (m *MapDB) GetNode(nodeKey []byte) (*Node, error) {
	var nk [12]byte
	copy(nk[:], nodeKey)
	n, ok := m.nodes[nk]
	if !ok {
		return nil, fmt.Errorf("MapDB: node not found")
	}
	return n, nil
}

type nodeCacheKey [12]byte

var _ NodeBackend = (*KeyValueBackend)(nil)

type KeyValueBackend struct {
	nodeCache NodeCache
	nodes     []*Node
	orphans   []*Node
	db        dbm.DB
	walBuf    *bytes.Buffer
	wal       *Wal
	walIdx    uint64

	// metrics
	MetricBlockCount      CountMetric
	MetricCacheSize       GaugeMetric
	MetricCacheMiss       CountMetric
	MetricCacheHit        CountMetric
	MetricDbFetch         CountMetric
	MetricDbFetchDuration HistogramMetric
}

func NewKeyValueBackend(db dbm.DB, cacheSize int, wal *Wal) (*KeyValueBackend, error) {
	walIdx, err := wal.FirstIndex()
	if err != nil {
		return nil, err
	}
	if walIdx == 0 {
		walIdx = 1
	}

	lruCache := NewNodeLruCache(cacheSize)

	return &KeyValueBackend{
		db:     db,
		wal:    wal,
		walIdx: walIdx,
		//nodeCache: &NodeMapCache{cache: make(map[nodeCacheKey]*Node)},
		nodeCache: lruCache,
		walBuf:    new(bytes.Buffer),
	}, nil
}

func (kv *KeyValueBackend) QueueNode(node *Node) error {
	if node.nodeKey == nil {
		return ErrNodeMissingNodeKey
	}
	kv.nodes = append(kv.nodes, node)
	return nil
}

func (kv *KeyValueBackend) QueueOrphan(node *Node) error {
	if node.nodeKey == nil {
		return ErrNodeMissingNodeKey
	}
	kv.orphans = append(kv.orphans, node)
	return nil
}

func (kv *KeyValueBackend) Commit(version int64) error {
	var nk nodeCacheKey

	changeset := &ChangeSet{}
	for _, node := range kv.nodes {
		//nodeBz, err := WriteWalNode(kv.walBuf, node, 0)
		//if err != nil {
		//	return err
		//}
		changeset.Pairs = append(changeset.Pairs, &KVPair{Key: node.key, Value: node.value})

		copy(nk[:], node.nodeKey.GetKey())
		dn := &deferredNode{nodeKey: nk, node: node}
		kv.wal.CachePut(dn)
	}

	for _, node := range kv.orphans {
		//nodeBz, err := WriteWalNode(kv.walBuf, node, 1)
		//if err != nil {
		//	return err
		//}
		copy(nk[:], node.nodeKey.GetKey())
		changeset.Pairs = append(changeset.Pairs, &KVPair{Key: node.key, Value: node.value, Delete: true})
		dn := &deferredNode{nodeKey: nk, deleted: true, node: node}
		kv.wal.CachePut(dn)
		kv.nodeCache.Remove(nk)
	}

	if kv.MetricCacheSize != nil {
		kv.MetricCacheSize.Set(float64(kv.nodeCache.Size()))
	}

	walBz, err := proto.Marshal(changeset)
	if err != nil {
		return err
	}
	kv.walBuf.Write(walBz)

	if kv.walBuf.Len() > 50*1024*1024 {
		err = kv.wal.Write(kv.walIdx, kv.walBuf.Bytes())
		if err != nil {
			return err
		}
		// TODO: support single threaded checkpoint by configuration
		//err = kv.wal.MaybeCheckpoint(kv.walIdx, version, kv.nodeCache)
		//if err != nil {
		//	return err
		//}
		kv.wal.checkpointCh <- &checkpointArgs{kv.walIdx, version, kv.nodeCache}
		kv.walBuf.Reset()
		kv.walIdx++
	}

	kv.nodes = nil
	kv.orphans = nil

	if kv.MetricBlockCount != nil {
		kv.MetricBlockCount.Inc()
	}

	return nil
}

func (kv *KeyValueBackend) GetNode(nodeKey []byte) (*Node, error) {
	var nk nodeCacheKey
	copy(nk[:], nodeKey)

	// fetch from wal cache
	node, err := kv.wal.CacheGet(nk)
	if err != nil {
		return nil, err
	}
	if node != nil {
		return node, nil
	}

	// fetch lru cache
	if n, ok := kv.nodeCache.Get(nk); ok {
		if kv.MetricCacheHit != nil {
			kv.MetricCacheHit.Inc()
		}
		return n, nil
	}
	if kv.MetricCacheMiss != nil {
		kv.MetricCacheMiss.Inc()
	}

	// fetch from commitment store
	if kv.MetricDbFetch != nil {
		kv.MetricDbFetch.Inc()
	}
	since := time.Now()
	value, err := kv.db.Get(nodeKey)
	if err != nil {
		return nil, err
	}
	if kv.MetricDbFetchDuration != nil {
		kv.MetricDbFetchDuration.Observe(time.Since(since).Seconds())
	}
	if value == nil {
		return nil, fmt.Errorf("kv/GetNode; node not found; nodeKey: %s [%X]", GetNodeKey(nodeKey), nodeKey)
	}
	node, err = MakeNode(nodeKey, value)
	if err != nil {
		return nil, fmt.Errorf("kv/GetNode/MakeNode; nodeKey: %s [%X]; bytes: %X; %w",
			GetNodeKey(nodeKey), nodeKey, value, err)
	}
	kv.nodeCache.Add(nk, node)
	return node, nil
}
