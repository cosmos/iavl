package iavl

import "fmt"

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
