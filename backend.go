package iavl

import (
	"bytes"
	"encoding/binary"
	"fmt"

	dbm "github.com/cosmos/cosmos-db"
	lru "github.com/hashicorp/golang-lru/v2"
)

type NodeBackend interface {
	// QueueNode queues a node for storage.
	QueueNode(*Node) error

	// QueueOrphan queues a node for orphaning.
	QueueOrphan(*Node) error

	// Commit commits all queued nodes to storage.
	Commit() error

	GetNode(nodeKey []byte) (*Node, error)
}

var _ NodeBackend = (*MapDB)(nil)

type MapDB struct {
	nodes map[[12]byte]*Node
}

func NewMapDB() *MapDB {
	return &MapDB{
		nodes: make(map[[12]byte]*Node),
	}
}

func (m MapDB) QueueNode(node *Node) error {
	var nk [12]byte
	copy(nk[:], node.nodeKey.GetKey())
	m.nodes[nk] = node
	return nil
}

func (m MapDB) QueueOrphan(node *Node) error {
	var nk [12]byte
	copy(nk[:], node.nodeKey.GetKey())
	delete(m.nodes, nk)
	return nil
}

func (m MapDB) Commit() error {
	return nil
}

func (m MapDB) GetNode(nodeKey []byte) (*Node, error) {
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
	mapCache map[nodeCacheKey]*Node
	cache    *lru.Cache[nodeCacheKey, *Node]
	nodes    []*Node
	orphans  []*Node
	db       dbm.DB
	walBuf   bytes.Buffer
	wal      *Wal
	walIdx   uint64

	// metrics
	MetricBlockCount CountMetric
	MetricCacheSize  GaugeMetric
}

func NewKeyValueBackend(db dbm.DB, cacheSize int, wal *Wal) (*KeyValueBackend, error) {
	cache, err := lru.New[nodeCacheKey, *Node](cacheSize)
	if err != nil {
		return nil, err
	}
	walIdx, err := wal.FirstIndex()
	if err != nil {
		return nil, err
	}
	if walIdx == 0 {
		walIdx = 1
	}

	return &KeyValueBackend{
		cache:    cache,
		db:       db,
		wal:      wal,
		walIdx:   walIdx,
		mapCache: map[nodeCacheKey]*Node{},
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

func (kv *KeyValueBackend) Commit() error {
	var nk nodeCacheKey
	var version int64

	for _, node := range kv.nodes {
		nkBz := node.nodeKey.GetKey()
		copy(nk[:], nkBz)
		//kv.cache.Add(nk, node)
		kv.mapCache[nk] = node

		if version == 0 {
			version = node.nodeKey.version
		}

		var nodeBuf bytes.Buffer
		if err := node.writeBytes(&nodeBuf); err != nil {
			return err
		}
		nodeBz := nodeBuf.Bytes()
		size := len(nodeBz)
		kv.walBuf.Grow(1 + 12 + 4 + size)
		// delete = false
		if err := kv.walBuf.WriteByte(0); err != nil {
			return err
		}
		// node key
		if _, err := kv.walBuf.Write(nkBz); err != nil {
			return err
		}
		// size
		if err := binary.Write(&kv.walBuf, binary.BigEndian, uint32(size)); err != nil {
			return err
		}
		// payload
		if _, err := kv.walBuf.Write(nodeBz); err != nil {
			return err
		}
	}

	for _, node := range kv.orphans {
		nkBz := node.nodeKey.GetKey()
		copy(nk[:], nkBz)
		//kv.cache.Remove(nk)
		delete(kv.mapCache, nk)

		var nodeBuf bytes.Buffer
		if err := node.writeBytes(&nodeBuf); err != nil {
			return err
		}
		nodeBz := nodeBuf.Bytes()
		size := len(nodeBz)
		kv.walBuf.Grow(1 + 12 + 4 + size)
		// delete = true
		if err := kv.walBuf.WriteByte(1); err != nil {
			return err
		}
		// node key
		if _, err := kv.walBuf.Write(nkBz); err != nil {
			return err
		}
		// size
		err := binary.Write(&kv.walBuf, binary.BigEndian, uint32(size))
		if err != nil {
			return err
		}
		// payload
		if _, err = kv.walBuf.Write(nodeBz); err != nil {
			return err
		}
	}

	if kv.MetricCacheSize != nil {
		kv.MetricCacheSize.Set(float64(len(kv.mapCache)))
	}

	if kv.walBuf.Len() > 70*1024*1024 {
		// TODO: send span mapping idx to height range
		err := kv.wal.Write(kv.walIdx, kv.walBuf.Bytes())
		if err != nil {
			return err
		}
		kv.walBuf.Reset()
		kv.walIdx++
	}

	// snapshot
	if kv.walIdx%100_000 == 0 {
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
	//if node, ok := kv.cache.Get(nk); ok {
	if node, ok := kv.mapCache[nk]; ok {
		return node, nil
	}

	return nil, fmt.Errorf("KeyValueBackend: node not found")

	// TODO sync with WAL
	//value, err := kv.db.Get(nodeKey)
	//if err != nil {
	//	return nil, err
	//}
	//node, err := MakeNode(nodeKey, value)
	//if err != nil {
	//	return nil, err
	//}
	//return node, nil
}
