package iavl

import (
	"fmt"
	"os"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
)

type sqliteDb struct {
	connString string
	write      *sqlite3.Conn
	read       *sqlite3.Conn

	pool *nodePool

	shardId      int64
	shards       map[int64]*sqlite3.Stmt
	versionShard map[int64]int64
}

func newSqliteDb(path string, newDb bool) (*sqliteDb, error) {
	sql := &sqliteDb{
		shards:       make(map[int64]*sqlite3.Stmt),
		versionShard: make(map[int64]int64),
		connString:   fmt.Sprintf("file:%s/iavl-v2.db", path),
	}

	var err error
	sql.write, err = sqlite3.Open(sql.connString)
	if err != nil {
		return nil, err
	}

	err = sql.write.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}

	// wal_autocheckpoint is in pages, so we need to convert maxWalSizeBytes to pages
	maxWalSizeBytes := 1024 * 1024 * 500
	if err = sql.write.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", maxWalSizeBytes/os.Getpagesize())); err != nil {
		return nil, err
	}

	if newDb {
		if err := sql.initNewDb(); err != nil {
			return nil, err
		}
	}

	//sql.treeInsert, err = sql.write.Prepare("INSERT INTO tree(node_key, bytes) VALUES (?, ?)")
	//if err != nil {
	//	return nil, err
	//}

	return sql, nil
}

func (sql *sqliteDb) newReadConn() (*sqlite3.Conn, error) {
	conn, err := sqlite3.Open(sql.connString)
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", 8*1024*1024*1024))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (sql *sqliteDb) resetReadConn() (err error) {
	if sql.read != nil {
		err = sql.read.Close()
		if err != nil {
			return err
		}
	}
	sql.read, err = sql.newReadConn()
	return err
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
CREATE TABLE shard (version int, id int, PRIMARY KEY (version, id));`)
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

	return nil
}

func (sql *sqliteDb) BatchSet(nodes []*Node) (n int64, versions []int64, err error) {
	batchSize := 200_000
	var byteCount int64
	versionMap := make(map[int64]bool)

	logger := log.With().Str("op", "batch-set").Logger()

	newBatch := func() (*sqlite3.Stmt, error) {
		var err error
		err = sql.write.Begin()
		if err != nil {
			return nil, err
		}

		var stmt *sqlite3.Stmt

		// sharded
		stmt, err = sql.write.Prepare(fmt.Sprintf(
			"INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", sql.shardId))

		// no Shard
		//stmt, err = sql.write.Prepare(fmt.Sprintf("INSERT INTO tree (sequence, version, bytes) VALUES (?, ?, ?)"))

		if err != nil {
			return nil, err
		}
		return stmt, nil
	}
	stmt, err := newBatch()
	if err != nil {
		return 0, versions, err
	}
	since := time.Now()
	for i, node := range nodes {
		versionMap[node.nodeKey.version] = true
		bz, err := node.Bytes()
		byteCount += int64(len(bz))
		if err != nil {
			return 0, versions, err
		}
		err = stmt.Exec(node.nodeKey.version, int(node.nodeKey.sequence), bz)
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

			logger.Info().Msgf("i=%s dur=%s rate=%s",
				humanize.Comma(int64(i)),
				time.Since(since).Round(time.Millisecond),
				humanize.Comma(int64(float64(batchSize)/time.Since(since).Seconds())))
			since = time.Now()
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
	err = sql.write.Exec(fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", sql.shardId, sql.shardId))
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

func (sql *sqliteDb) GetShardQuery(version int64) (*sqlite3.Stmt, error) {
	id, ok := sql.versionShard[version]
	if !ok {
		return nil, fmt.Errorf("shard not found for version %d", version)
	}
	q, ok := sql.shards[id]
	if !ok {
		return nil, fmt.Errorf("shard query not found for id %d", id)
	}
	return q, nil
}

func (sql *sqliteDb) getNode(nodeKey *NodeKey, q *sqlite3.Stmt) (*Node, error) {
	key := nodeKey.GetKey()
	if err := q.Bind(nodeKey.version, int(nodeKey.sequence)); err != nil {
		return nil, err
	}
	hasRow, err := q.Step()
	if !hasRow {
		return nil, fmt.Errorf("node not found: %v; shard=%d", GetNodeKey(key),
			sql.versionShard[nodeKey.version])
	}
	if err != nil {
		return nil, err
	}
	nodeBz, err := q.ColumnBlob(0)
	node, err := MakeNode(sql.pool, key, nodeBz)
	if err != nil {
		return nil, err
	}
	err = q.Reset()
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (sql *sqliteDb) Get(nodeKey *NodeKey) (*Node, error) {
	q, err := sql.GetShardQuery(nodeKey.version)
	if err != nil {
		return nil, err
	}
	return sql.getNode(nodeKey, q)
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

	defer stmt.Close()
	for _, version := range versions {
		err := stmt.Exec(version, shardId)
		if err != nil {
			return err
		}
		sql.versionShard[version] = shardId
	}
	return sql.write.Commit()
}

func (sql *sqliteDb) NextShard() error {
	// initialize shardId if not done so. done with a new connection.
	if sql.shardId == 0 {
		conn, err := sqlite3.Open(sql.connString)
		if err != nil {
			return err
		}
		q, err := conn.Prepare("SELECT MAX(id) FROM shard")
		if err != nil {
			return err
		}
		_, err = q.Step()
		if err != nil {
			return err
		}

		// if table is empty MAX query will bind sql.shardId to zero
		err = q.Scan(&sql.shardId)
		if err != nil {
			return err
		}

		if err := q.Close(); err != nil {
			return err
		}
		if err := conn.Close(); err != nil {
			return err
		}
	}

	sql.shardId++

	// hack to maintain 1 shard for testing
	//if sql.shardId > 1 {
	//	sql.shardId = 1
	//	return nil
	//}

	log.Info().Msgf("creating shard %d", sql.shardId)

	err := sql.write.Exec(fmt.Sprintf("CREATE TABLE tree_%d (version int, sequence int, bytes blob);",
		sql.shardId))
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
	conn, err := sql.newReadConn()
	if err != nil {
		return nil, err
	}
	rootQuery, err := conn.Prepare("SELECT node_version, node_sequence FROM root WHERE version = ?", version)
	if err != nil {
		return nil, err
	}

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
	if err := rootQuery.Close(); err != nil {
		return nil, err
	}

	// TODO this placement seems wrong?
	if err := sql.resetShardQueries(); err != nil {
		return nil, err
	}

	rootNode, err := sql.Get(root)
	if err != nil {
		return nil, err
	}
	if err := conn.Close(); err != nil {
		return nil, err
	}
	return rootNode, nil
}

func (sql *sqliteDb) addShardQuery() error {
	if _, ok := sql.shards[sql.shardId]; ok {
		return nil
	}
	if sql.read == nil {
		if err := sql.resetReadConn(); err != nil {
			return err
		}
	}

	q, err := sql.read.Prepare(fmt.Sprintf(
		"SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ?", sql.shardId))
	if err != nil {
		return err
	}
	sql.shards[sql.shardId] = q
	return nil
}

func (sql *sqliteDb) resetShardQueries() error {
	for _, q := range sql.shards {
		err := q.Close()
		if err != nil {
			return err
		}
	}

	if sql.read == nil {
		// single reader conn for all shards. keep open to fill mmap, but reset queries periodically to flush WAL
		if err := sql.resetReadConn(); err != nil {
			return err
		}
	}

	q, err := sql.read.Prepare("SELECT DISTINCT id FROM shard")
	if err != nil {
		return err
	}
	for {
		ok, err := q.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		var shardId int64
		err = q.Scan(&shardId)
		if err != nil {
			return err
		}
		sql.shards[shardId], err = sql.read.Prepare(
			fmt.Sprintf("SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ?", shardId))
		if err != nil {
			return err
		}
	}
	err = q.Close()
	if err != nil {
		return err
	}

	q, err = sql.read.Prepare("SELECT version, id FROM shard")
	for {
		ok, err := q.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		var version, shardId int64
		err = q.Scan(&version, &shardId)
		if err != nil {
			return err
		}
		sql.versionShard[version] = shardId
	}

	return q.Close()
}
