package iavl

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/golang-lru/v2"
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
	dbDir            string
	wal              *os.File

	deleteStmt  *stmtConn
	insertStmt  *stmtConn
	getNodeStmt *stmtConn

	ctx         context.Context
	resetPool   chan *stmtConn
	getNodePool chan *stmtConn
	cache       *lru.Cache[[12]byte, *Node]
}

func (db *SqliteDb) StoreChangeSet(version int64, changeset *ChangeSet) error {
	if len(changeset.Pairs) == 0 {
		return nil
	}

	if err := db.storage.Begin(); err != nil {
		return err
	}

	storeLeaf, err := db.storage.Prepare(`INSERT INTO leaf(key, value, version, deleted) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	storeTree, err := db.storage.Prepare(`INSERT INTO tree (node_key, bytes) VALUES (?, ?)`)
	if err != nil {
		return err
	}

	for _, pair := range changeset.Pairs {
		// TODO 4 cases
		if pair.Delete {
			if err := storeLeaf.Exec(pair.Key, pair.Value, version, true); err != nil {
				return err
			}
		} else {
			if err := storeTree.Exec(pair.Key, pair.Value); err != nil {
				return err
			}
		}
	}

	return db.Commit()
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

func (db *SqliteDb) Commit() error {
	return db.cacheCommit()
}

func (db *SqliteDb) cacheCommit() error {
	var nk [12]byte

	changeSet := &ChangeSet{}

	for _, node := range db.batch {
		copy(nk[:], node.nodeKey.GetKey())
		db.cache.Add(nk, node)
		changeSet.Pairs = append(changeSet.Pairs, &KVPair{Key: node.key, Value: node.value})
	}

	for _, node := range db.orphans {
		copy(nk[:], node.nodeKey.GetKey())
		db.cache.Remove(nk)
		changeSet.Pairs = append(changeSet.Pairs, &KVPair{Key: node.key, Value: node.value, Delete: true})
	}

	db.batch = nil
	db.orphans = nil

	if len(changeSet.Pairs) > 0 {
		bz, err := proto.Marshal(changeSet)
		if err != nil {
			return err
		}
		_, err = db.wal.Write(bz)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *SqliteDb) dbCommit() error {
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
	//orphans, err := db.latest.Prepare(`DELETE FROM node WHERE node_key = ?`)
	//if err != nil {
	//	return err
	//}

	var nk [12]byte
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

		copy(nk[:], node.nodeKey.GetKey())
		db.cache.Add(nk, node)

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
		//if err = orphans.Exec(node.nodeKey.GetKey()); err != nil {
		//	return err
		//}
		copy(nk[:], node.nodeKey.GetKey())
		db.cache.Remove(nk)
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

	var nkbz [12]byte
	copy(nkbz[:], nk)
	cacheNode, found := db.cache.Get(nkbz)
	if found {
		return cacheNode, nil
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
	lruCache, err := lru.New[[12]byte, *Node](1_000_000)
	if err != nil {
		return nil, err
	}
	db := &SqliteDb{
		ctx:   ctxt,
		cache: lruCache,
		dbDir: path,
	}

	newDb := false
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		err := os.Mkdir(path, 0755)
		if err != nil {
			return nil, err
		}
		db.wal, err = os.Create(fmt.Sprintf("%s/wal", path))
		if err != nil {
			return nil, err
		}
		newDb = true
	}

	db.latestConnString = fmt.Sprintf("file:%s/latest.db?cache=shared", path)
	//db.latestConnString = "file::memory:?cache=shared"

	db.latest, err = sqlite3.Open(db.latestConnString)
	if err != nil {
		return nil, err
	}
	err = db.latest.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	// wal_autocheckpoint is in pages, so we need to convert maxWalSizeBytes to pages
	maxWalSizeBytes := 1024 * 1024 * 1024 * 3
	if err = db.latest.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", maxWalSizeBytes/os.Getpagesize())); err != nil {
		//if err = db.latest.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", 0)); err != nil {
		return nil, err
	}
	db.storage, err = sqlite3.Open(fmt.Sprintf("file:%s/storage.db?cache=shared", path))
	if err != nil {
		return nil, err
	}
	err = db.storage.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}

	if newDb {
		if err := db.initNewDb(); err != nil {
			return nil, err
		}
	}

	maxSqlConn := 1
	db.getNodePool = make(chan *stmtConn, maxSqlConn)
	db.resetPool = make(chan *stmtConn, maxSqlConn)
	for i := 0; i < maxSqlConn; i++ {
		conn, err := sqlite3.Open(db.latestConnString)
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

		//db.getNodePool <- &stmtConn{stmt, conn}
		db.getNodeStmt = &stmtConn{stmt, conn}

		conn, err = sqlite3.Open(db.latestConnString)
		err = conn.Exec("PRAGMA synchronous=OFF;")
		if err != nil {
			return nil, err
		}
		stmt, err = conn.Prepare(`INSERT INTO node(node_key, bytes) VALUES (?, ?)`)
		if err != nil {
			return nil, err
		}
		db.insertStmt = &stmtConn{stmt, conn}

		conn, err = sqlite3.Open(db.latestConnString)
		err = conn.Exec("PRAGMA synchronous=OFF;")
		if err != nil {
			return nil, err
		}
		stmt, err = conn.Prepare(`DELETE FROM node WHERE node_key = ?`)
		if err != nil {
			return nil, err
		}
		db.deleteStmt = &stmtConn{stmt, conn}
	}

	//for i := 0; i < 8; i++ {
	//	go func() {
	//		for {
	//			select {
	//			case <-ctxt.Done():
	//				break
	//			case sc := <-db.resetPool:
	//				err := sc.Reset()
	//				if err != nil {
	//					// TODO
	//					panic(err)
	//				}
	//				db.getNodePool <- sc
	//			}
	//		}
	//	}()
	//}

	return db, nil
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
		CREATE TABLE node(node_key blob, bytes blob)
`)
	if err != nil {
		return err
	}
	err = db.storage.Exec(`
CREATE TABLE node
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
CREATE TABLE tree (node_key blob, bytes blob, PRIMARY KEY (node_key));`)
	if err != nil {
		return err
	}

	pagesize := os.Getpagesize()

	if err = db.latest.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pagesize)); err != nil {
		return err
	}
	if err = db.latest.Exec("PRAGMA journal_mode=WAL;"); err != nil {
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
