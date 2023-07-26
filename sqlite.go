package iavl

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

// TODO
// optimizations:
// - push deleted leaves into storage
// - keep a copy of `nodes` in latest and prune periodically
// - WAL mode on both dbs
// - auto WAL checkpointing disabled on storage, periodic in background thread

type SqliteDb struct {
	batch   []*Node
	latest  *sql.DB
	storage *sql.DB
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
	storageTx, err := db.storage.Begin()
	if err != nil {
		return err
	}
	leavesTx, err := db.latest.Begin()
	if err != nil {
		return err
	}
	storage, err := storageTx.Prepare(
		`INSERT INTO node(version, sequence, key, hash, size, height, left_version, left_sequence, right_version, right_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	leaves, err := leavesTx.Prepare(
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
		_, err = storage.Exec(node.nodeKey.version, node.nodeKey.nonce, node.key, hash, node.size, node.subtreeHeight,
			leftNodeKey.version, leftNodeKey.nonce, rightNodeKey.version, rightNodeKey.nonce)
		if err != nil {
			return err
		}
		if node.isLeaf() {
			_, err = leaves.Exec(node.key, node.value, node.nodeKey.version, 0)
			if err != nil {
				return err
			}
		}
	}
	err = storageTx.Commit()
	if err != nil {
		return err
	}
	err = leavesTx.Commit()
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

	// this seems wrong
	nodeKey := GetNodeKey(nk)
	v := int(nodeKey.version)
	seq := int(nodeKey.nonce)
	rows := db.storage.QueryRow(
		`SELECT 
    height, size, key, hash, left_version, left_sequence, right_version, right_sequence
FROM node WHERE version = ? AND sequence = ?`, v, seq)

	n := &Node{
		nodeKey: nodeKey,
	}
	err := rows.Scan(&n.subtreeHeight, &n.size, &n.key, &n.hash,
		&leftNodeKey.version, &leftNodeKey.nonce,
		&rightNodeKey.version, &rightNodeKey.nonce)
	if err != nil {
		return nil, err
	}
	if n.isLeaf() {
		n._hash(nodeKey.version)
	} else {
		n.leftNodeKey = leftNodeKey.GetKey()
		n.rightNodeKey = rightNodeKey.GetKey()
	}
	return n, nil
}

func NewSqliteDb(path string) (*SqliteDb, error) {
	sqlDb := &SqliteDb{}
	newDb := false
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		err := os.Mkdir(path, 0755)
		if err != nil {
			return nil, err
		}
		newDb = true
	}

	sqlDb.latest, err = sql.Open("sqlite3", path+"/latest.db")
	if err != nil {
		return nil, err
	}
	sqlDb.storage, err = sql.Open("sqlite3", path+"/storage.db")
	if err != nil {
		return nil, err
	}

	if newDb {
		_, err = sqlDb.latest.Exec(`
		CREATE TABLE leaf
		(
			 key   blob
			,value blob
			,version int
			,deleted int
		);`)
		if err != nil {
			return nil, err
		}
		_, err := sqlDb.storage.Exec(`
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
			return nil, err
		}
	}
	_, err = sqlDb.latest.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return nil, err
	}
	_, err = sqlDb.latest.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	_, err = sqlDb.latest.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return nil, err
	}
	_, err = sqlDb.latest.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 1024*1024*1024*5))
	if err != nil {
		return nil, err
	}

	_, err = sqlDb.storage.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return nil, err
	}
	_, err = sqlDb.storage.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	_, err = sqlDb.storage.Exec("PRAGMA temp_store=MEMORY;")
	if err != nil {
		return nil, err
	}
	_, err = sqlDb.storage.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 1024*1024*1024*5))
	if err != nil {
		return nil, err
	}
	return sqlDb, nil
}

func (db *SqliteDb) Close() error {
	err := db.latest.Close()
	if err != nil {
		return err
	}
	return db.storage.Close()
}
