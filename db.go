package iavl

import "crypto/sha256"

const (
	hashSize = sha256.Size
)

type DB interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Delete(key []byte) error
}

type nodeDB interface {
	Set(node *Node)
	Get(nk NodeKey) *Node
	Delete(nk NodeKey)
}

type kvDB struct {
	db             DB
	lastCheckpoint int64
}

func (kv *kvDB) Set(node *Node) {
}

// mapDB approximates a database with a map.
// it used to store nodes in memory so that pool size can be constrained and tested.
type mapDB struct {
	nodes          map[NodeKey]Node
	setCount       int
	deleteCount    int
	lastCheckpoint int64
}

func newMapDB() *mapDB {
	return &mapDB{
		nodes: make(map[NodeKey]Node),
	}
}

func (db *mapDB) Set(node *Node) {
	nk := *node.NodeKey
	n := *node
	n.overflow = false
	n.dirty = false
	n.leftNode = nil
	n.rightNode = nil
	n.frameId = -1
	db.nodes[nk] = n
	db.setCount++
}

func (db *mapDB) Get(nk NodeKey) *Node {
	n, ok := db.nodes[nk]
	if !ok {
		return nil
	}
	return &n
}

func (db *mapDB) Delete(nk NodeKey) {
	delete(db.nodes, nk)
	db.deleteCount++
}
