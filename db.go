package iavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
)

const (
	hashSize = sha256.Size
)

type DB interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Delete(key []byte) error
}

type nodeDB interface {
	Set(node *Node) error
	Get(nk NodeKey) (*Node, error)
	Delete(nk NodeKey) error
}

type KvDB struct {
	db   DB
	pool *NodePool

	readCount    int64
	readTime     time.Duration
	readLeafMiss int64
}

func NewKvDB(db DB, pool *NodePool) *KvDB {
	return &KvDB{
		db:   db,
		pool: pool,
	}
}

func (kv *KvDB) setLeaf(node *Node) error {
	var k bytes.Buffer
	k.Write([]byte("l"))
	k.Write(node.nodeKey[:])
	return kv.setNode(k.Bytes(), node)
}

func (kv *KvDB) getLeaf(nodeKey NodeKey) (*Node, error) {
	return kv.getNode([]byte("l"), nodeKey)
}

func (kv *KvDB) setBranch(node *Node) error {
	var k bytes.Buffer
	k.Write([]byte("b"))
	k.Write(node.nodeKey[:])
	return kv.setNode(k.Bytes(), node)
}

func (kv *KvDB) getBranch(nodeKey NodeKey) (*Node, error) {
	return kv.getNode([]byte("b"), nodeKey)
}

func (kv *KvDB) setRoot(node *Node, version int64) error {
	var k bytes.Buffer
	k.Write([]byte("r"))
	if err := binary.Write(&k, binary.BigEndian, version); err != nil {
		return err
	}
	return kv.db.Set(k.Bytes(), node.nodeKey[:])
}

func (kv *KvDB) setNode(key []byte, node *Node) error {
	bz, err := node.Bytes()
	if err != nil {
		return err
	}
	return kv.db.Set(key, bz)
}

func (kv *KvDB) getNode(prefix []byte, nodeKey NodeKey) (*Node, error) {
	start := time.Now()
	var k bytes.Buffer
	k.Write(prefix)
	k.Write(nodeKey[:])

	bz, err := kv.db.Get(k.Bytes())
	if err != nil {
		return nil, err
	}
	if bz == nil {
		return nil, nil
	}
	n, err := MakeNode(kv.pool, nodeKey, bz)
	if err != nil {
		return nil, err
	}
	kv.readCount++
	kv.readTime += time.Since(start)
	return n, nil
}

func (kv *KvDB) getRoot(version int64) (*Node, error) {
	var k bytes.Buffer
	k.Write([]byte("r"))
	if err := binary.Write(&k, binary.BigEndian, version); err != nil {
		return nil, err
	}
	nkbz, err := kv.db.Get(k.Bytes())
	if err != nil {
		return nil, err
	}
	if nkbz == nil {
		return nil, fmt.Errorf("root not found: %d", version)
	}
	nk := NodeKey{}
	copy(nk[:], nkbz)
	return kv.getBranch(nk)
}

func (kv *KvDB) Set(node *Node) (int, error) {
	bz, err := node.Bytes()
	if err != nil {
		return 0, err
	}
	return len(bz), kv.db.Set(node.nodeKey[:], bz)
}

func (kv *KvDB) Get(nodeKey NodeKey) (*Node, error) {
	bz, err := kv.db.Get(nodeKey[:])
	if err != nil {
		return nil, err
	}
	if bz == nil {
		return nil, fmt.Errorf("node not found: %v", nodeKey)
	}
	n, err := MakeNode(kv.pool, nodeKey, bz)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (kv *KvDB) Delete(nodeKey NodeKey) error {
	return kv.db.Delete(nodeKey[:])
}

func (kv *KvDB) getRightNode(node *Node) (*Node, error) {
	var err error
	if node.subtreeHeight == 1 || node.subtreeHeight == 2 {
		node.rightNode, err = kv.getLeaf(node.rightNodeKey)
		if err != nil {
			return nil, err
		}
		if node.rightNode != nil {
			return node.rightNode, nil
		}
	}
	kv.readLeafMiss++

	node.rightNode, err = kv.getBranch(node.rightNodeKey)
	if err != nil {
		return nil, err
	}
	return node.rightNode, nil
}

func (kv *KvDB) getLeftNode(node *Node) (*Node, error) {
	var err error
	if node.subtreeHeight == 1 || node.subtreeHeight == 2 {
		node.leftNode, err = kv.getLeaf(node.leftNodeKey)
		if err != nil {
			return nil, err
		}
		if node.leftNode != nil {
			return node.leftNode, nil
		}
	}
	kv.readLeafMiss++

	node.leftNode, err = kv.getBranch(node.leftNodeKey)
	if err != nil {
		return nil, err
	}
	return node.leftNode, err
}

func (kv *KvDB) readReport() error {
	if kv.readCount == 0 {
		return nil
	}

	fmt.Printf("reads=%s r/s=%s dur/q=%s leaf-miss=%s, dur=%s\n",
		humanize.Comma(kv.readCount),
		humanize.Comma(int64(float64(kv.readCount)/kv.readTime.Seconds())),
		time.Duration(int64(kv.readTime)/kv.readCount),
		humanize.Comma(kv.readLeafMiss),
		kv.readTime.Round(time.Millisecond),
	)

	kv.readCount, kv.readTime, kv.readLeafMiss = 0, 0, 0

	return nil
}
