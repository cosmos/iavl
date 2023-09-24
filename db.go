package iavl

import (
	"crypto/sha256"
	"fmt"
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

type kvDB struct {
	db   DB
	pool *NodePool
}

func (kv *kvDB) Set(node *Node) (int, error) {
	bz, err := node.Bytes()
	if err != nil {
		return 0, err
	}
	return len(bz), kv.db.Set(node.nodeKey[:], bz)
}

func (kv *kvDB) Get(nodeKey NodeKey) (*Node, error) {
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

func (kv *kvDB) Delete(nodeKey NodeKey) error {
	return kv.db.Delete(nodeKey[:])
}
