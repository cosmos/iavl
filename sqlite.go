package iavl

import (
	"fmt"
	"os"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
)

type sqliteDb struct {
	write      *sqlite3.Conn
	treeInsert *sqlite3.Stmt
	treeQuery  *sqlite3.Stmt
	treeDelete *sqlite3.Stmt

	nodeInsert *sqlite3.Stmt
	nodeQuery  *sqlite3.Stmt

	pool *nodePool

	shardId      int64
	shards       map[int64]*sqlite3.Stmt
	versionShard map[int64]int64
}

func newSqliteDb(path string, newDb bool) (*sqliteDb, error) {
	sql := &sqliteDb{
		shards:       make(map[int64]*sqlite3.Stmt),
		versionShard: make(map[int64]int64),
	}
	connString := fmt.Sprintf("file:%s/iavl-v2.db?cache=shared", path)

	var err error
	sql.write, err = sqlite3.Open(connString)
	if err != nil {
		return nil, err
	}

	err = sql.write.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 3*1024*1024*1024))
	if err != nil {
		return nil, err
	}

	//err = sql.write.Exec("PRAGMA synchronous=OFF;")
	//if err != nil {
	//	return nil, err
	//}
	if newDb {
		err = sql.write.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			return nil, err
		}
		if err := sql.initNewDb(); err != nil {
			return nil, err
		}
	}

	// wal_autocheckpoint is in pages, so we need to convert maxWalSizeBytes to pages
	maxWalSizeBytes := 1024 * 1024 * 500
	if err = sql.write.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", maxWalSizeBytes/os.Getpagesize())); err != nil {
		return nil, err
	}

	sql.treeInsert, err = sql.write.Prepare("INSERT INTO tree(node_key, bytes) VALUES (?, ?)")
	if err != nil {
		return nil, err
	}
	sql.treeQuery, err = sql.write.Prepare("SELECT bytes FROM tree WHERE node_key = ?")
	if err != nil {
		return nil, err
	}

	return sql, nil
}

func (sql *sqliteDb) initNewDb() error {
	err := sql.write.Exec(`
CREATE TABLE node
		(
			 seq   int
			,version int
		    ,hash blob
			,key blob
		    ,height int
			,size int
			,l_seq int
		    ,l_version int
			,r_seq int
			,r_version int
		);
CREATE TABLE tree (node_key blob, bytes blob);
CREATE TABLE shard (version int, id string, PRIMARY KEY (version, id));`)
	if err != nil {
		return err
	}

	pagesize := os.Getpagesize()

	err = sql.write.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pagesize))
	if err != nil {
		return err
	}
	err = sql.write.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}

	pageSize := os.Getpagesize()
	log.Info().Msgf("setting page size to %s", humanize.Bytes(uint64(pageSize)))
	err = sql.write.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
	if err != nil {
		return err
	}

	return nil
}

func (sql *sqliteDb) Set(node *Node) (int, error) {
	bz, err := node.Bytes()
	if err != nil {
		return 0, err
	}
	err = sql.treeInsert.Exec(node.nodeKey.GetKey(), bz)
	if err != nil {
		return 0, err
	}
	return len(bz), nil
}

func (sql *sqliteDb) BatchSet(nodes []*Node) (n int64, versions []int64, err error) {
	batchSize := 200_000
	var byteCount int64
	versionMap := make(map[int64]bool)

	newBatch := func() (*sqlite3.Stmt, error) {
		var err error
		err = sql.write.Begin()
		if err != nil {
			return nil, err
		}

		var stmt *sqlite3.Stmt
		stmt, err = sql.write.Prepare(fmt.Sprintf("INSERT INTO tree_%d (node_key, bytes, seq, version) VALUES (?, ?, ?, ?)", sql.shardId))
		//stmt, err = sql.write.Prepare("INSERT INTO node(seq, version, hash, key, height, size, l_seq, l_version, r_seq, r_version) " +
		//	"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

		if err != nil {
			return nil, err
		}
		return stmt, nil
	}
	stmt, err := newBatch()
	if err != nil {
		return 0, versions, err
	}
	for i, node := range nodes {
		if node.nodeKey.String() == "(10110, 697)" {
			fmt.Println("here")
		}
		versionMap[node.nodeKey.version] = true
		bz, err := node.Bytes()
		byteCount += int64(len(bz))
		if err != nil {
			return 0, versions, err
		}
		err = stmt.Exec(node.nodeKey.GetKey(), bz, int(node.nodeKey.sequence), node.nodeKey.version)
		if err != nil {
			return 0, versions, err
		}
		if i%batchSize == 0 {
			err := sql.write.Commit()
			if err != nil {
				return 0, versions, err
			}
			err = stmt.Close()
			if err != nil {
				return 0, versions, err
			}
			stmt, err = newBatch()
			if err != nil {
				return 0, versions, err
			}
		}
		sql.pool.Put(node)
	}
	err = sql.write.Commit()
	if err != nil {
		return 0, versions, err
	}
	err = stmt.Close()
	if err != nil {
		return 0, versions, err
	}

	for version := range versionMap {
		versions = append(versions, version)
	}
	return byteCount, versions, nil
}

func (sql *sqliteDb) GetShardConn(version int64) (*sqlite3.Stmt, error) {
	id, ok := sql.versionShard[version]
	if !ok {
		return nil, fmt.Errorf("shard not found for version %d", version)
	}
	conn, ok := sql.shards[id]
	if !ok {
		return nil, fmt.Errorf("shard connection not found for version %d", version)
	}
	return conn, nil
}

func (sql *sqliteDb) Get(nodeKey *NodeKey) (*Node, error) {
	//conn, err := sql.GetShardConn(nodeKey.version)
	shardId := sql.versionShard[nodeKey.version]
	conn, err := sql.write.Prepare(fmt.Sprintf("SELECT bytes FROM tree_%d WHERE node_key = ?", shardId))
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	key := nodeKey.GetKey()
	if err := conn.Bind(key); err != nil {
		return nil, err
	}
	hasRow, err := conn.Step()
	if !hasRow {
		return nil, fmt.Errorf("node not found: %v; shard=%d", GetNodeKey(key),
			sql.versionShard[nodeKey.version])
	}
	if err != nil {
		return nil, err
	}
	nodeBz, err := conn.ColumnBlob(0)
	node, err := MakeNode(sql.pool, key, nodeBz)
	if err != nil {
		return nil, err
	}
	err = conn.Reset()
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (sql *sqliteDb) Delete(nodeKey []byte) error {
	return nil
}

func (sql *sqliteDb) Close() error {
	// TODO close all connections
	return sql.write.Close()
}

func (sql *sqliteDb) MapVersions(versions []int64, shardId int64) error {
	err := sql.write.Begin()
	if err != nil {
		return err
	}
	stmt, err := sql.write.Prepare("INSERT INTO shard(version, id) VALUES (?, ?)")
	if err != nil {
		return err
	}

	queryStmt, err := sql.write.Prepare(fmt.Sprintf("SELECT bytes FROM tree_%d WHERE node_key = ?", shardId))
	if err != nil {
		return err
	}

	defer stmt.Close()
	for _, version := range versions {
		err := stmt.Exec(version, shardId)
		if err != nil {
			return err
		}
		sql.versionShard[version] = shardId
		sql.shards[version] = queryStmt
	}
	return sql.write.Commit()
}

func (sql *sqliteDb) CreateShard() error {
	sql.shardId++
	err := sql.write.Exec(fmt.Sprintf("CREATE TABLE tree_%d (node_key blob, bytes blob, seq int, version int);", sql.shardId))
	if err != nil {
		return err
	}
	return err
}

func (sql *sqliteDb) IndexShard(shardId int64) error {
	err := sql.write.Exec(fmt.Sprintf("CREATE INDEX tree_%d_node_key_idx ON tree_%d (node_key);", shardId, shardId))
	return err
}
