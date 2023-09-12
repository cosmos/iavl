package v6

type DB interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Delete(key []byte) error
}

type nodeDB interface {
	Set(node *Node)
	Get(nk nodeKey) *Node
	Delete(nk nodeKey)
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
	nodes          map[nodeKey]Node
	setCount       int
	deleteCount    int
	lastCheckpoint int64
}

func newMapDB() *mapDB {
	return &mapDB{
		nodes: make(map[nodeKey]Node),
	}
}

func (db *mapDB) Set(node *Node) {
	nk := *node.nodeKey
	n := *node
	n.overflow = false
	n.dirty = false
	n.leftNode = nil
	n.rightNode = nil
	n.frameId = -1
	db.nodes[nk] = n
	db.setCount++
}

func (db *mapDB) Get(nk nodeKey) *Node {
	n, ok := db.nodes[nk]
	if !ok {
		return nil
	}
	return &n
}

func (db *mapDB) Delete(nk nodeKey) {
	delete(db.nodes, nk)
	db.deleteCount++
}
