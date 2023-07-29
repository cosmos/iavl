package iavl

import (
	"bytes"
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
	batch            []*Node
	orphans          []*Node
	latest           *sqlite3.Conn
	storage          *sqlite3.Conn
	latestConnString string

	deleteStmt  *stmtConn
	insertStmt  *stmtConn
	getNodeStmt *stmtConn

	ctx         context.Context
	resetPool   chan *stmtConn
	getNodePool chan *stmtConn
}

func (db *SqliteDb) QueueOrphan(node *Node) error {
	if node.nodeKey == nil {
		return ErrNodeMissingNodeKey
	}
	db.orphans = append(db.orphans, node)
	return nil
}

func (db *SqliteDb) QueueNode(node *Node) error {
	if node.nodeKey == nil {
		return ErrNodeMissingNodeKey
	}
	db.batch = append(db.batch, node)
	return nil
}

func (db *SqliteDb) PreparedCommit() error {
	for _, node := range db.batch {
		var buf bytes.Buffer
		buf.Grow(node.encodedSize())
		if err := node.writeBytes(&buf); err != nil {
			return err
		}
		err := db.insertStmt.Exec(node.nodeKey.GetKey(), buf.Bytes())
		if err != nil {
			return err
		}
	}

	for _, node := range db.orphans {
		err := db.deleteStmt.Exec(node.nodeKey.GetKey())
		if err != nil {
			return err
		}
	}

	db.batch = nil
	db.orphans = nil
	return nil
}

func (db *SqliteDb) Commit() error {
	var err error
	//err = db.storage.Begin()
	//if err != nil {
	//	return err
	//}
	err = db.latest.Begin()
	if err != nil {
		return err
	}

	//storageNode, err := db.storage.Prepare(`INSERT INTO node(node_key, bytes) VALUES (?, ?)`)
	//if err != nil {
	//	return err
	//}
	//storageTree, err := db.storage.Prepare(
	//	`INSERT INTO tree(seq, version, hash, height, size, l_seq, l_version, r_seq, r_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	//if err != nil {
	//	return err
	//}
	latestNode, err := db.latest.Prepare(`INSERT INTO node(node_key, bytes) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	orphans, err := db.latest.Prepare(`DELETE FROM node WHERE node_key = ?`)
	if err != nil {
		return err
	}

	for _, node := range db.batch {
		var buf bytes.Buffer
		buf.Grow(node.encodedSize())
		if err = node.writeBytes(&buf); err != nil {
			return err
		}

		//if err = storageNode.Exec(node.nodeKey.GetKey(), buf.Bytes()); err != nil {
		//	return err
		//}
		if err = latestNode.Exec(node.nodeKey.GetKey(), buf.Bytes()); err != nil {
			return err
		}

		//var leftNodeKey NodeKey
		//var rightNodeKey NodeKey
		//if !node.IsLeaf() {
		//	leftNodeKey = *GetNodeKey(node.leftNodeKey)
		//	rightNodeKey = *GetNodeKey(node.rightNodeKey)
		//}
		//err = storageTree.Exec(
		//	int(node.nodeKey.nonce), int(node.nodeKey.version), node.hash, int(node.subtreeHeight), int(node.size),
		//	int(leftNodeKey.nonce), int(leftNodeKey.version), int(rightNodeKey.nonce), int(rightNodeKey.version))
		//if err != nil {
		//	return err
		//}
	}

	for _, node := range db.orphans {
		if err = orphans.Exec(node.nodeKey.GetKey()); err != nil {
			return err
		}
	}

	//err = db.storage.Commit()
	//if err != nil {
	//	return err
	//}
	err = db.latest.Commit()
	if err != nil {
		return err
	}

	db.batch = nil
	db.orphans = nil

	return nil
}

func (db *SqliteDb) GetNode(nk []byte) (*Node, error) {
	if nk == nil {
		return nil, ErrNodeMissingNodeKey
	}
	//leftNodeKey := &NodeKey{}
	//rightNodeKey := &NodeKey{}

	//nodeKey := GetNodeKey(nk)
	//v := int(nodeKey.version)
	//seq := int(nodeKey.nonce)
	//k := (int(nodeKey.version) << 32) | int(nodeKey.nonce)
	//stmt := <-db.getNodePool
	stmt := db.getNodeStmt
	err := stmt.Bind(nk)
	if err != nil {
		return nil, err
	}

	ok, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("node not found")
	}

	var bz sqlite3.RawBytes
	err = stmt.Scan(&bz)
	if err != nil {
		return nil, err
	}
	n, err := MakeNode(nk, bz)
	if err != nil {
		return nil, err
	}

	//if n.isLeaf() {
	//	n._hash(nodeKey.version)
	//} else {
	//	n.leftNodeKey = leftNodeKey.GetKey()
	//	n.rightNodeKey = rightNodeKey.GetKey()
	//}

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

	//sqlDb.latestConnString = fmt.Sprintf("file:%s/latest.db?cache=shared", path)
	sqlDb.latestConnString = "file::memory:?cache=shared"

	sqlDb.latest, err = sqlite3.Open(sqlDb.latestConnString)
	if err != nil {
		return nil, err
	}
	err = sqlDb.latest.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	sqlDb.storage, err = sqlite3.Open(fmt.Sprintf("file:%s/storage.db?cache=shared", path))
	if err != nil {
		return nil, err
	}
	err = sqlDb.storage.Exec("PRAGMA synchronous=OFF;")
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
		conn, err := sqlite3.Open(sqlDb.latestConnString)
		if err != nil {
			return nil, err
		}
		err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 1024*1024*1024))
		if err != nil {
			return nil, err
		}

		stmt, err := conn.Prepare(`SELECT bytes FROM node WHERE node_key = ?`)
		if err != nil {
			return nil, err
		}

		//sqlDb.getNodePool <- &stmtConn{stmt, conn}
		sqlDb.getNodeStmt = &stmtConn{stmt, conn}

		conn, err = sqlite3.Open(sqlDb.latestConnString)
		err = conn.Exec("PRAGMA synchronous=OFF;")
		if err != nil {
			return nil, err
		}
		stmt, err = conn.Prepare(`INSERT INTO node(node_key, bytes) VALUES (?, ?)`)
		if err != nil {
			return nil, err
		}
		sqlDb.insertStmt = &stmtConn{stmt, conn}

		conn, err = sqlite3.Open(sqlDb.latestConnString)
		err = conn.Exec("PRAGMA synchronous=OFF;")
		if err != nil {
			return nil, err
		}
		stmt, err = conn.Prepare(`DELETE FROM node WHERE node_key = ?`)
		if err != nil {
			return nil, err
		}
		sqlDb.deleteStmt = &stmtConn{stmt, conn}
	}

	//for i := 0; i < 8; i++ {
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
		CREATE TABLE node(node_key blob, bytes blob, PRIMARY KEY (node_key));
`)
	if err != nil {
		return err
	}
	err = db.storage.Exec(`
CREATE TABLE tree
		(
			 seq   int
			,version int
		    ,hash blob
		    ,height int
			,size int
			,l_seq int
		    ,l_version int
			,r_seq int
			,r_version int
		);
CREATE TABLE leaf (key blob, value blob, deleted int, version int, PRIMARY KEY (key));
CREATE TABLE node (node_key blob, bytes blob, PRIMARY KEY (node_key));`)
	if err != nil {
		return err
	}

	pagesize := os.Getpagesize()

	err = db.latest.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pagesize))
	if err != nil {
		return err
	}
	err = db.latest.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}

	err = db.storage.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pagesize))
	if err != nil {
		return err
	}
	err = db.storage.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}

	return nil
}
