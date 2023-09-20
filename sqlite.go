package iavl

import (
	"fmt"
	"os"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
)

type sqliteDb struct {
	write *sqlite3.Conn
	read  *sqlite3.Conn

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
	connString := fmt.Sprintf("file:%s/iavl-v2.db", path)

	var err error
	sql.write, err = sqlite3.Open(connString)
	if err != nil {
		return nil, err
	}

	err = sql.write.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}

	if newDb {
		if err := sql.initNewDb(); err != nil {
			return nil, err
		}
	}

	sql.read, err = sqlite3.Open(connString)
	if err != nil {
		return nil, err
	}

	//sql.treeInsert, err = sql.write.Prepare("INSERT INTO tree(node_key, bytes) VALUES (?, ?)")
	//if err != nil {
	//	return nil, err
	//}

	err = sql.read.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 8*1024*1024*1024))
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
CREATE TABLE root (version int, node_version int, node_sequence, PRIMARY KEY (version));
CREATE TABLE tree (version int, sequence int, bytes blob);
CREATE TABLE shard (version int, id string, PRIMARY KEY (version, id));`)
	if err != nil {
		return err
	}

	pageSize := os.Getpagesize()
	log.Info().Msgf("setting page size to %s", humanize.Bytes(uint64(pageSize)))
	err = sql.write.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
	if err != nil {
		return err
	}
	err = sql.write.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}

	// wal_autocheckpoint is in pages, so we need to convert maxWalSizeBytes to pages
	maxWalSizeBytes := 1024 * 1024 * 500
	if err = sql.write.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", maxWalSizeBytes/pageSize)); err != nil {
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

	//logger := log.With().Str("op", "batch-set").Logger()

	newBatch := func() (*sqlite3.Stmt, error) {
		var err error
		err = sql.write.Begin()
		if err != nil {
			return nil, err
		}

		var stmt *sqlite3.Stmt

		//stmt, err = sql.write.Prepare(fmt.Sprintf(
		//	"INSERT INTO tree_%d (node_key, bytes, seq, version) VALUES (?, ?, ?, ?)", sql.shardId))

		stmt, err = sql.write.Prepare(fmt.Sprintf("INSERT INTO tree (sequence, version, bytes) VALUES (?, ?, ?)"))

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
	//since := time.Now()
	for i, node := range nodes {
		versionMap[node.nodeKey.version] = true
		bz, err := node.Bytes()
		byteCount += int64(len(bz))
		if err != nil {
			return 0, versions, err
		}
		err = stmt.Exec(int(node.nodeKey.sequence), node.nodeKey.version, bz)
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

			//logger.Info().Msgf("i=%s dur=%s rate=%s",
			//	humanize.Comma(int64(i)),
			//	time.Since(since).Round(time.Millisecond),
			//	humanize.Comma(int64(float64(batchSize)/time.Since(since).Seconds())))
			//since = time.Now()
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

	// TODO
	err = sql.write.Exec("CREATE INDEX IF NOT EXISTS tree_idx ON tree (version, sequence);")
	if err != nil {
		return 0, versions, err
	}

	err = sql.write.Exec("PRAGMA wal_checkpoint(RESTART);")
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
	//shardId := sql.versionShard[nodeKey.version]
	//conn, err := sql.write.Prepare(fmt.Sprintf("SELECT bytes FROM tree_%d WHERE node_key = ?", shardId))
	//defer conn.Close()
	//if err != nil {
	//	return nil, err
	//}

	conn := sql.treeQuery

	key := nodeKey.GetKey()
	if err := conn.Bind(nodeKey.version, int(nodeKey.sequence)); err != nil {
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

func (sql *sqliteDb) SaveRoot(version int64, node *Node) error {
	err := sql.write.Exec("INSERT INTO root(version, node_version, node_sequence) VALUES (?, ?, ?)",
		version, node.nodeKey.version, int(node.nodeKey.sequence))
	return err
}

func (sql *sqliteDb) LoadRoot(version int64) (*Node, error) {
	rootQuery, err := sql.read.Prepare("SELECT node_version, node_sequence FROM root WHERE version = ?", version)
	if err != nil {
		return nil, err
	}
	defer rootQuery.Close()
	hasRow, err := rootQuery.Step()
	if !hasRow {
		return nil, fmt.Errorf("root not found for version %d", version)
	}
	if err != nil {
		return nil, err
	}
	root := &NodeKey{}
	var seq int
	err = rootQuery.Scan(&root.version, &seq)
	if err != nil {
		return nil, err
	}
	root.sequence = uint32(seq)
	err = sql.resetTreeQuery()
	if err != nil {
		return nil, err
	}
	return sql.Get(root)
}

func (sql *sqliteDb) resetTreeQuery() error {
	var err error
	if sql.treeQuery != nil {
		err := sql.treeQuery.Close()
		if err != nil {
			return err
		}
	}
	sql.treeQuery, err = sql.read.Prepare("SELECT bytes FROM tree WHERE version = ? AND sequence = ?")
	return err
}
