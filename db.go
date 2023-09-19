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
	Get(nk *NodeKey) (*Node, error)
	Delete(nk *NodeKey) error
}

type kvDB struct {
	db DB
}

func (kv *kvDB) Set(node *Node) error {
	bz, err := node.Bytes()
	if err != nil {
		return err
	}
	return kv.db.Set(node.nodeKey.GetKey(), bz)
}

func (kv *kvDB) Get(nodeKey *NodeKey) (*Node, error) {
	nk := nodeKey.GetKey()
	bz, err := kv.db.Get(nk)
	if err != nil {
		return nil, err
	}
	if bz == nil {
		return nil, fmt.Errorf("node not found: %s", nodeKey.String())
	}
	n, err := MakeNode(nk, bz)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (kv *kvDB) Delete(nk *NodeKey) error {
	return kv.db.Delete(nk.GetKey())
}
