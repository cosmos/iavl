package iavl

import (
	"context"
	"fmt"
	"os"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

var (
	getNodeCount = 0
	getNodeTime  = int64(0)
)

// TODO
// optimizations:
// - push deleted leaves into storage
// - keep a copy of `nodes` in latest and prune periodically
// - WAL mode on both dbs
// - auto WAL checkpointing disabled on storage, periodic in background thread

type stmtConn struct {
	*sqlite3.Stmt
	conn *sqlite3.Conn
}

type SqliteDb struct {
	batch   []*Node
	latest  *sqlite3.Conn
	storage *sqlite3.Conn

	getNodeStmt *stmtConn
	ctx         context.Context
	resetPool   chan *stmtConn
	getNodePool chan *stmtConn
}

func (db *SqliteDb) QueueNode(node *Node) error {
	if node.nodeKey == nil {
		return ErrNodeMissingNodeKey
	}
	db.batch = append(db.batch, node)
	return nil
}

func (db *SqliteDb) Commit() error {
	var leftNodeKey, rightNodeKey *NodeKey
	err := db.storage.Begin()
	if err != nil {
		return err
	}
	err = db.latest.Begin()
	if err != nil {
		return err
	}

	storage, err := db.storage.Prepare(
		`INSERT INTO node(version, sequence, key, hash, size, height, left_version, left_sequence, right_version, right_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	leaves, err := db.latest.Prepare(
		`INSERT INTO leaf(key, value, version, deleted) VALUES(?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	for _, node := range db.batch {
		if node.subtreeHeight > 0 {
			leftNodeKey = GetNodeKey(node.leftNodeKey)
			rightNodeKey = GetNodeKey(node.rightNodeKey)
		} else {
			// leaf
			leftNodeKey = &NodeKey{}
			rightNodeKey = &NodeKey{}
		}

		hash := node._hash(node.nodeKey.version)
		err = storage.Exec(node.nodeKey.version, int(node.nodeKey.nonce), node.key, hash, node.size, int(node.subtreeHeight),
			leftNodeKey.version, int(leftNodeKey.nonce), rightNodeKey.version, int(rightNodeKey.nonce))
		if err != nil {
			return err
		}
		if node.isLeaf() {
			err = leaves.Exec(node.key, node.value, node.nodeKey.version, 0)
			if err != nil {
				return err
			}
		}
	}
	err = db.storage.Commit()
	if err != nil {
		return err
	}
	err = db.latest.Commit()
	if err != nil {
		return err
	}

	db.batch = nil
	return nil
}

func (db *SqliteDb) GetNode(nk []byte) (*Node, error) {
	if nk == nil {
		return nil, ErrNodeMissingNodeKey
	}
	leftNodeKey := &NodeKey{}
	rightNodeKey := &NodeKey{}

	nodeKey := GetNodeKey(nk)
	v := int(nodeKey.version)
	seq := int(nodeKey.nonce)
	//stmt := <-db.getNodePool
	stmt := db.getNodeStmt
	err := stmt.Bind(v, seq)
	if err != nil {
		return nil, err
	}
	n := &Node{
		nodeKey: nodeKey,
	}
	ok, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("node not found")
	}

	var height, leftNonce, rightNonce int
	err = stmt.Scan(&height, &n.size, &n.key, &n.hash,
		&leftNodeKey.version, &leftNonce,
		&rightNodeKey.version, &rightNonce)
	if err != nil {
		return nil, err
	}

	n.subtreeHeight = int8(height)
	leftNodeKey.nonce = uint32(leftNonce)
	rightNodeKey.nonce = uint32(rightNonce)

	if n.isLeaf() {
		n._hash(nodeKey.version)
	} else {
		n.leftNodeKey = leftNodeKey.GetKey()
		n.rightNodeKey = rightNodeKey.GetKey()
	}

	//if err := stmt.Reset(); err != nil {
	//	return nil, err
	//}

	if err = stmt.Reset(); err != nil {
		return nil, err
	}

	//db.getNodePool <- stmt
	//db.resetPool <- stmt

	return n, nil
}

func NewSqliteDb(ctxt context.Context, path string) (*SqliteDb, error) {
	sqlDb := &SqliteDb{ctx: ctxt}
	newDb := false
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		err := os.Mkdir(path, 0755)
		if err != nil {
			return nil, err
		}
		newDb = true
	}

	sqlDb.latest, err = sqlite3.Open(fmt.Sprintf("file:%s/latest.db?cache=shared", path))
	if err != nil {
		return nil, err
	}
	sqlDb.storage, err = sqlite3.Open(fmt.Sprintf("file:%s/storage.db?cache=shared", path))
	if err != nil {
		return nil, err
	}

	if newDb {
		if err := sqlDb.initNewDb(); err != nil {
			return nil, err
		}
	}

	maxSqlConn := 1
	sqlDb.getNodePool = make(chan *stmtConn, maxSqlConn)
	sqlDb.resetPool = make(chan *stmtConn, maxSqlConn)
	for i := 0; i < maxSqlConn; i++ {
		conn, err := sqlite3.Open(fmt.Sprintf("file:%s/storage.db?cache=shared", path))
		if err != nil {
			return nil, err
		}
		err = conn.Exec("PRAGMA synchronous=OFF;")
		if err != nil {
			return nil, err
		}
		err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 1024*1024*1024))
		if err != nil {
			return nil, err
		}
		stmt, err := conn.Prepare(
			`SELECT 
    height, size, key, hash, left_version, left_sequence, right_version, right_sequence
FROM node WHERE version = ? AND sequence = ?`)
		if err != nil {
			return nil, err
		}
		//sqlDb.getNodePool <- &stmtConn{stmt, conn}
		sqlDb.getNodeStmt = &stmtConn{stmt, conn}
	}

	//for i := 0; i < 2; i++ {
	//	go func() {
	//		for {
	//			select {
	//			case <-ctxt.Done():
	//				break
	//			case sc := <-sqlDb.resetPool:
	//				err := sc.Reset()
	//				if err != nil {
	//					// TODO
	//					panic(err)
	//				}
	//				sqlDb.getNodePool <- sc
	//			}
	//		}
	//	}()
	//}

	return sqlDb, nil
}

func (db *SqliteDb) Close() error {
	db.ctx.Done()
	close(db.getNodePool)
	for c := range db.getNodePool {
		if err := c.Close(); err != nil {
			return err
		}
		if err := c.conn.Close(); err != nil {
			return err
		}
	}
	if err := db.storage.Close(); err != nil {
		// TODO
		return nil
	}
	if err := db.latest.Close(); err != nil {
		return err
	}
	return nil
}

func (db *SqliteDb) initNewDb() error {
	err := db.latest.Exec(`
		CREATE TABLE leaf
		(
			 key   blob
			,value blob
			,version int
			,deleted int
		);`)
	if err != nil {
		return err
	}
	err = db.storage.Exec(`
CREATE TABLE node (
   version int,
   sequence int,
   key blob,
   hash blob,
   size int,
   height int,
   left_version int,
   left_sequence int,
   right_version int,
   right_sequence int,
   PRIMARY KEY (version, sequence)
);`)
	if err != nil {
		return err
	}

	err = db.latest.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}

	err = db.storage.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}

	return nil
}
