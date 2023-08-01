package iavl

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/gogo/protobuf/proto"
	"github.com/tidwall/wal"
)

type CountMetric interface {
	Inc()
}

type GaugeMetric interface {
	Add(float64)
	Sub(float64)
	Set(float64)
}

type HistogramMetric interface {
	Observe(float64)
}

type NaiveWal struct {
	logDir string
}

var _ proto.Message = (*WalNode)(nil)

type WalNode struct {
	Height   int64  `protobuf:"varint,1,opt,name=height,proto3" json:"height,omitempty"`
	Size     int64  `protobuf:"varint,2,opt,name=size,proto3" json:"size,omitempty"`
	Key      []byte `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	Value    []byte `protobuf:"bytes,4,opt,name=value,proto3" json:"value,omitempty"`
	Hash     []byte `protobuf:"bytes,5,opt,name=hash,proto3" json:"hash,omitempty"`
	LeftKey  []byte `protobuf:"bytes,6,opt,name=left_key,proto3" json:"left_key,omitempty"`
	RightKey []byte `protobuf:"bytes,7,opt,name=right_key,proto3" json:"right_key,omitempty"`
}

func (w *WalNode) Reset() { *w = WalNode{} }

func (w *WalNode) String() string { return "" }

func (w *WalNode) ProtoMessage() {}

func WriteWalNodeProto(node *Node) ([]byte, error) {
	walNode := &WalNode{
		Height: int64(node.subtreeHeight),
		Size:   node.size,
		Key:    node.key,
	}
	if node.isLeaf() {
		walNode.Value = node.value
	} else {
		walNode.Hash = node.hash
		walNode.LeftKey = node.leftNodeKey
		walNode.RightKey = node.rightNodeKey
	}
	return proto.Marshal(walNode)
}

func WriteWalNode(w *bytes.Buffer, node *Node, delete byte) ([]byte, error) {
	if delete != 0 && delete != 1 {
		return nil, fmt.Errorf("delete must be 0 or 1")
	}
	if err := w.WriteByte(delete); err != nil {
		return nil, err
	}

	// node key
	var nk nodeCacheKey
	nkBz := node.nodeKey.GetKey()
	copy(nk[:], nkBz)

	if _, err := w.Write(nkBz); err != nil {
		return nil, err
	}

	if delete == 1 {
		return nil, nil
	}

	// size
	var nodeBuf bytes.Buffer
	if err := node.writeBytes(&nodeBuf); err != nil {
		return nil, err
	}
	nodeBz := nodeBuf.Bytes()
	size := len(nodeBz)
	if err := binary.Write(w, binary.BigEndian, uint32(size)); err != nil {
		return nil, err
	}

	// payload
	if _, err := w.Write(nodeBz); err != nil {
		return nil, err
	}
	return nodeBz, nil
}

func NewTidwalLog(logDir string) (*wal.Log, error) {
	walOpts := wal.DefaultOptions
	walOpts.NoSync = true
	walOpts.NoCopy = true
	log, err := wal.Open(fmt.Sprintf("%s/iavl.wal", logDir), walOpts)
	return log, err
}

type walCache struct {
	puts         map[nodeCacheKey]*deferredNode
	deletes      []*deferredNode
	sinceVersion int64
}

type checkpointArgs struct {
	index   uint64
	version int64
	cache   NodeCache
}

type Wal struct {
	wal                *wal.Log
	commitment         dbm.DB
	storage            *SqliteDb
	checkpointInterval int
	checkpointHead     uint64
	checkpointCh       chan *checkpointArgs

	cacheLock sync.RWMutex
	hotCache  *walCache
	coldCache *walCache

	MetricNodesRead CountMetric
	MetricWalSize   GaugeMetric
	MetricCacheMiss CountMetric
	MetricCacheHit  CountMetric
	MetricCacheSize GaugeMetric
}

func NewWal(wal *wal.Log, commitment dbm.DB, storage *SqliteDb) *Wal {
	hot := &walCache{
		puts:    make(map[nodeCacheKey]*deferredNode),
		deletes: []*deferredNode{},
	}
	cold := &walCache{
		puts:    make(map[nodeCacheKey]*deferredNode),
		deletes: []*deferredNode{},
	}
	return &Wal{
		wal:                wal,
		commitment:         commitment,
		storage:            storage,
		hotCache:           hot,
		coldCache:          cold,
		checkpointCh:       make(chan *checkpointArgs, 100),
		checkpointInterval: 100,
	}
}

func (r *Wal) Write(idx uint64, bz []byte) error {
	if r.MetricWalSize != nil {
		r.MetricWalSize.Add(float64(len(bz)))
	}
	return r.wal.Write(idx, bz)
}

func (r *Wal) CacheGet(key nodeCacheKey) (*Node, error) {
	r.cacheLock.RLock()
	hot := *r.hotCache
	cold := *r.coldCache
	r.cacheLock.RUnlock()

	if dn, ok := hot.puts[key]; ok {
		if r.MetricCacheHit != nil {
			r.MetricCacheHit.Inc()
		}
		return dn.node, nil
	}

	if dn, ok := cold.puts[key]; ok {
		if r.MetricCacheHit != nil {
			r.MetricCacheHit.Inc()
		}
		return dn.node, nil
	}

	if r.MetricCacheMiss != nil {
		r.MetricCacheMiss.Inc()
	}
	return nil, nil
}

func (r *Wal) CachePut(node *deferredNode) {
	r.cacheLock.Lock()
	cache := r.hotCache
	r.cacheLock.Unlock()

	nk := node.nodeKey

	if !node.deleted {
		cache.puts[nk] = node
	} else {
		delete(cache.puts, nk)
		nodeKey := GetNodeKey(nk[:])
		if nodeKey.version < cache.sinceVersion {
			cache.deletes = append(cache.deletes, node)
		}
	}
	if r.MetricNodesRead != nil {
		r.MetricNodesRead.Inc()
	}
	if r.MetricCacheSize != nil {
		r.MetricCacheSize.Set(float64(len(cache.puts)))
	}
}

func (r *Wal) FirstIndex() (uint64, error) {
	return r.wal.FirstIndex()
}

type deferredNode struct {
	nodeBz  *[]byte
	node    *Node
	nodeKey nodeCacheKey
	deleted bool
}

func (r *Wal) CheckpointRunner(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case args := <-r.checkpointCh:
			if r.checkpointHead == 0 {
				r.checkpointHead = args.index
			}

			if args.index-r.checkpointHead >= uint64(r.checkpointInterval) {
				r.cacheLock.Lock()
				r.hotCache, r.coldCache = r.coldCache, r.hotCache
				r.hotCache.sinceVersion = args.version + 1
				r.cacheLock.Unlock()
				err := r.Checkpoint(args.index, args.version, args.cache)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (r *Wal) Checkpoint(index uint64, version int64, cache NodeCache) error {
	start := time.Now()
	fmt.Printf("wal: checkpointing now. [%d - %d) will be flushed to state commitment\n",
		r.checkpointHead, index)
	buf := new(bytes.Buffer)
	for k, dn := range r.coldCache.puts {
		err := dn.node.writeBytes(buf)
		if err != nil {
			return err
		}
		err = r.commitment.Set(k[:], buf.Bytes())
		if err != nil {
			return err
		}
		cache.Add(k, dn.node)
		buf.Reset()
	}
	for _, dn := range r.coldCache.deletes {
		err := r.commitment.Delete(dn.nodeKey[:])
		if err != nil {
			return err
		}
	}
	if err := r.wal.TruncateFront(index); err != nil {
		return err
	}

	r.cacheLock.Lock()
	//r.cache = make(map[nodeCacheKey]*deferredNode)
	r.coldCache = &walCache{
		puts:    make(map[nodeCacheKey]*deferredNode),
		deletes: []*deferredNode{},
	}
	r.cacheLock.Unlock()

	//if r.MetricWalSize != nil {
	//	r.MetricWalSize.Sub(checkpointBz)
	//}
	if r.MetricCacheSize != nil {
		r.MetricCacheSize.Set(0)
	}
	r.checkpointHead = index
	//checkpointBz = 0
	fmt.Printf("wal: checkpoint completed in %.3fs\n", time.Since(start).Seconds())
	return nil
}

func (r *Wal) MaybeCheckpoint(index uint64, version int64, cache NodeCache) error {
	if r.checkpointHead == 0 {
		r.checkpointHead = index
	}

	if index-r.checkpointHead >= uint64(r.checkpointInterval) {
		return r.Checkpoint(index, version, cache)
	}

	return nil
}
