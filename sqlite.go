package iavl

import (
	"fmt"
	"os"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"
	"github.com/rs/zerolog"
)

type SqliteDbOptions struct {
	Path      string
	Mode      int
	MmapSize  int
	WalSize   int
	CacheSize int
	ConnArgs  string
	Metrics   *metrics.TreeMetrics
}

type SqliteDb struct {
	opts SqliteDbOptions

	// 2 separate databases and 2 separate connections.  the underlying databases have different WAL policies
	// therefore separation is required.
	leafWrite *sqlite3.Conn
	treeWrite *sqlite3.Conn

	itrIdx    int
	iterators map[int]*sqlite3.Stmt

	readConn *sqlite3.Conn

	pool *NodePool

	shardId      int64
	shards       map[int64]*sqlite3.Stmt
	versionShard map[int64]int64

	queryLatest *sqlite3.Stmt
	queryLeaf   *sqlite3.Stmt

	metrics *metrics.TreeMetrics
	logger  zerolog.Logger
}

func defaultSqliteDbOptions(opts SqliteDbOptions) SqliteDbOptions {
	if opts.Path == "" {
		opts.Path = "/tmp/iavl-v2"
	}
	if opts.MmapSize == 0 {
		opts.MmapSize = 8 * 1024 * 1024 * 1024
	}
	// if opts.ConnArgs == "" {
	// 	opts.ConnArgs = "cache=shared"
	// }
	if opts.Metrics == nil {
		opts.Metrics = &metrics.TreeMetrics{}
	}
	if opts.WalSize == 0 {
		opts.WalSize = 1024 * 1024 * 100
	}
	return opts
}

func (opts SqliteDbOptions) connArgs() string {
	if opts.ConnArgs == "" {
		return ""
	}
	return fmt.Sprintf("?%s", opts.ConnArgs)
}

func (opts SqliteDbOptions) leafConnectionString() string {
	return fmt.Sprintf("file:%s/changelog.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) treeConnectionString() string {
	return fmt.Sprintf("file:%s/tree.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) EstimateMmapSize() (int, error) {
	logger := log.With().Str("path", opts.Path).Logger()
	logger.Info().Msgf("calculate mmap size")
	logger.Info().Msgf("leaf connection string: %s", opts.leafConnectionString())
	// mmap leaf table size * 1.5
	conn, err := sqlite3.Open(opts.leafConnectionString())
	if err != nil {
		return 0, err
	}
	q, err := conn.Prepare("SELECT SUM(pgsize) FROM dbstat WHERE name = 'leaf'")
	if err != nil {
		return 0, err
	}
	hasRow, err := q.Step()
	if err != nil {
		return 0, err
	}
	if !hasRow {
		return 0, fmt.Errorf("no row")
	}
	var leafSize int
	err = q.Scan(&leafSize)
	if err != nil {
		return 0, err
	}
	if err = q.Close(); err != nil {
		return 0, err
	}
	if err = conn.Close(); err != nil {
		return 0, err
	}
	mmapSize := int(float64(leafSize) * 2)
	logger.Info().Msgf("leaf mmap size: %s", humanize.Bytes(uint64(mmapSize)))

	return mmapSize, nil
}

func NewInMemorySqliteDb(pool *NodePool) (*SqliteDb, error) {
	opts := defaultSqliteDbOptions(SqliteDbOptions{ConnArgs: "mode=memory"})
	return NewSqliteDb(pool, opts)
}

func NewSqliteDb(pool *NodePool, opts SqliteDbOptions) (*SqliteDb, error) {
	opts = defaultSqliteDbOptions(opts)
	logger := log.With().Str("module", "sqlite").Str("path", opts.Path).Logger()
	sql := &SqliteDb{
		shards:       make(map[int64]*sqlite3.Stmt),
		versionShard: make(map[int64]int64),
		iterators:    make(map[int]*sqlite3.Stmt),
		opts:         opts,
		pool:         pool,
		metrics:      opts.Metrics,
		logger:       logger,
	}

	if !api.IsFileExistent(opts.Path) {
		if err := os.MkdirAll(opts.Path, 0755); err != nil {
			return nil, err
		}
	}

	var err error
	sql.leafWrite, err = sqlite3.Open(opts.leafConnectionString())
	if err != nil {
		return nil, err
	}
	sql.treeWrite, err = sqlite3.Open(opts.treeConnectionString())
	if err != nil {
		return nil, err
	}

	err = sql.leafWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	err = sql.treeWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}

	if err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", opts.WalSize/os.Getpagesize())); err != nil {
		return nil, err
	}

	if err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", opts.WalSize/os.Getpagesize())); err != nil {
		return nil, err
	}

	if err = sql.init(); err != nil {
		return nil, err
	}

	return sql, nil
}

func (sql *SqliteDb) init() error {
	q, err := sql.treeWrite.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='root'")
	if err != nil {
		return err
	}
	hasRow, err := q.Step()
	if err != nil {
		return err
	}
	if !hasRow {
		err = sql.treeWrite.Exec(`
CREATE TABLE root (version int, node_version int, node_sequence int, bytes blob, checkpoint bool, PRIMARY KEY (version));
CREATE TABLE shard (version int, id int, PRIMARY KEY (version, id));`)
		if err != nil {
			return err
		}

		pageSize := os.Getpagesize()
		log.Info().Msgf("setting page size to %s", humanize.Bytes(uint64(pageSize)))
		err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = sql.treeWrite.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			return err
		}
	}
	if err = q.Close(); err != nil {
		return err
	}

	q, err = sql.leafWrite.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='leaf'")
	if err != nil {
		return err
	}
	if !hasRow {
		err = sql.leafWrite.Exec(`
CREATE TABLE latest (key blob, value blob, PRIMARY KEY (key));
CREATE TABLE leaf (version int, sequence int, bytes blob);
CREATE TABLE leaf_delete (version int, sequence int, key blob, PRIMARY KEY (version, sequence));`)
		if err != nil {
			return err
		}

		pageSize := os.Getpagesize()
		log.Info().Msgf("setting page size to %s", humanize.Bytes(uint64(pageSize)))
		err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		err = sql.leafWrite.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			return err
		}
	}
	if err = q.Close(); err != nil {
		return err
	}

	return nil
}

func (sql *SqliteDb) newReadConn() (*sqlite3.Conn, error) {
	conn, err := sqlite3.Open(sql.opts.treeConnectionString())
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s/changelog.sqlite' AS changelog;", sql.opts.Path))
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", sql.opts.MmapSize))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (sql *SqliteDb) resetReadConn() (err error) {
	if sql.readConn != nil {
		err = sql.readConn.Close()
		if err != nil {
			return err
		}
	}
	sql.readConn, err = sql.newReadConn()
	return err
}

func (sql *SqliteDb) getReadConn() (*sqlite3.Conn, error) {
	var err error
	if sql.readConn == nil {
		sql.readConn, err = sql.newReadConn()
	}
	return sql.readConn, err
}

func (sql *SqliteDb) GetShardQuery(version int64) (*sqlite3.Stmt, error) {
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

func (sql *SqliteDb) getLeaf(nodeKey NodeKey) (*Node, error) {
	start := time.Now()

	var err error
	if sql.queryLeaf == nil {
		sql.queryLeaf, err = sql.readConn.Prepare("SELECT bytes FROM changelog.leaf WHERE version = ? AND sequence = ?")
		if err != nil {
			return nil, err
		}
	}
	if err = sql.queryLeaf.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}
	hasRow, err := sql.queryLeaf.Step()
	if !hasRow {
		return nil, sql.queryLeaf.Reset()
	}
	if err != nil {
		return nil, err
	}
	var nodeBz sqlite3.RawBytes
	err = sql.queryLeaf.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}
	node, err := MakeNode(sql.pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}
	err = sql.queryLeaf.Reset()
	if err != nil {
		return nil, err
	}

	dur := time.Since(start)
	sql.metrics.QueryDurations = append(sql.metrics.QueryDurations, dur)
	sql.metrics.QueryTime += dur
	sql.metrics.QueryCount++
	sql.metrics.QueryLeafCount++

	return node, nil
}

func (sql *SqliteDb) getNode(nodeKey NodeKey, q *sqlite3.Stmt) (*Node, error) {
	start := time.Now()

	if err := q.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}
	hasRow, err := q.Step()
	if !hasRow {
		return nil, fmt.Errorf("node not found: %v; shard=%d", nodeKey, sql.versionShard[nodeKey.Version()])
	}
	if err != nil {
		return nil, err
	}
	var nodeBz sqlite3.RawBytes
	err = q.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}
	node, err := MakeNode(sql.pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}
	err = q.Reset()
	if err != nil {
		return nil, err
	}

	dur := time.Since(start)
	sql.metrics.QueryDurations = append(sql.metrics.QueryDurations, dur)
	sql.metrics.QueryTime += dur
	sql.metrics.QueryCount++
	sql.metrics.QueryBranchCount++

	return node, nil
}

func (sql *SqliteDb) Get(nodeKey NodeKey) (*Node, error) {
	q, err := sql.GetShardQuery(nodeKey.Version())
	if err != nil {
		return nil, err
	}
	return sql.getNode(nodeKey, q)
}

func (sql *SqliteDb) Delete(nodeKey []byte) error {
	return nil
}

func (sql *SqliteDb) Close() error {
	for _, q := range sql.shards {
		err := q.Close()
		if err != nil {
			return err
		}
	}
	if sql.readConn != nil {
		if sql.queryLeaf != nil {
			if err := sql.queryLeaf.Close(); err != nil {
				return err
			}
		}
		if err := sql.readConn.Close(); err != nil {
			return err
		}
	}
	if err := sql.leafWrite.Close(); err != nil {
		return err
	}

	if err := sql.treeWrite.Close(); err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) MapVersions(versions []int64, shardId int64) error {
	err := sql.treeWrite.Begin()
	if err != nil {
		return err
	}
	stmt, err := sql.treeWrite.Prepare("INSERT INTO shard(version, id) VALUES (?, ?)")
	if err != nil {
		return err
	}

	for _, version := range versions {
		err := stmt.Exec(version, shardId)
		if err != nil {
			return err
		}
		sql.versionShard[version] = shardId
	}
	if err = sql.treeWrite.Commit(); err != nil {
		return err
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) NextShard() error {
	// initialize shardId if not done so. done with a new connection.
	if sql.shardId == 0 {
		conn, err := sqlite3.Open(sql.opts.treeConnectionString())
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
	// if sql.shardId > 1 {
	//	sql.shardId = 1
	//	return nil
	// }

	sql.logger.Info().Msgf("creating shard %d", sql.shardId)

	err := sql.treeWrite.Exec(fmt.Sprintf("CREATE TABLE tree_%d (version int, sequence int, bytes blob);",
		sql.shardId))
	if err != nil {
		return err
	}
	return err
}

func (sql *SqliteDb) IndexShard(shardId int64) error {
	err := sql.leafWrite.Exec(fmt.Sprintf("CREATE INDEX tree_%d_node_key_idx ON tree_%d (node_key);", shardId, shardId))
	return err
}

func (sql *SqliteDb) SaveRoot(version int64, node *Node, isCheckpoint bool) error {
	bz, err := node.Bytes()
	if err != nil {
		return err
	}
	return sql.treeWrite.Exec("INSERT INTO root(version, node_version, node_sequence, bytes, checkpoint) VALUES (?, ?, ?, ?, ?)",
		version, node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz, isCheckpoint)
}

func (sql *SqliteDb) LoadRoot(version int64) (*Node, error) {
	conn, err := sqlite3.Open(sql.opts.treeConnectionString())
	if err != nil {
		return nil, err
	}
	rootQuery, err := conn.Prepare("SELECT node_version, node_sequence, bytes FROM root WHERE version = ?", version)
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
	var (
		nodeSeq     int
		nodeVersion int64
		nodeBz      []byte
	)
	err = rootQuery.Scan(&nodeVersion, &nodeSeq, &nodeBz)
	if err != nil {
		return nil, err
	}
	rootKey := NewNodeKey(nodeVersion, uint32(nodeSeq))
	root, err := MakeNode(sql.pool, rootKey, nodeBz)
	if err != nil {
		return nil, err
	}

	if err := rootQuery.Close(); err != nil {
		return nil, err
	}
	// TODO this placement seems wrong?
	if err := sql.resetShardQueries(); err != nil {
		return nil, err
	}
	if err := conn.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

// lastCheckpoint fetches the last checkpoint version from the shard table previous to the loaded root's version.
func (sql *SqliteDb) lastCheckpoint(treeVersion int64) (version int64, err error) {
	conn, err := sqlite3.Open(sql.opts.treeConnectionString())
	if err != nil {
		return 0, err
	}
	rootQuery, err := conn.Prepare("SELECT MAX(version) FROM root WHERE checkpoint = true AND version <= ?", treeVersion)
	if err != nil {
		return 0, err
	}
	hasRow, err := rootQuery.Step()
	if err != nil {
		return 0, err
	}
	if !hasRow {
		return 0, nil
	}
	err = rootQuery.Scan(&version)
	if err != nil {
		return 0, err
	}

	if err = rootQuery.Close(); err != nil {
		return 0, err
	}
	if err = conn.Close(); err != nil {
		return 0, err
	}
	return version, nil
}

func (sql *SqliteDb) addShardQuery() error {
	if _, ok := sql.shards[sql.shardId]; ok {
		return nil
	}
	if sql.readConn == nil {
		var err error
		sql.readConn, err = sql.getReadConn()
		if err != nil {
			return err
		}
	}

	q, err := sql.readConn.Prepare(fmt.Sprintf(
		"SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ?", sql.shardId))
	if err != nil {
		return err
	}
	sql.shards[sql.shardId] = q
	return nil
}

func (sql *SqliteDb) resetShardQueries() error {
	for _, q := range sql.shards {
		err := q.Close()
		if err != nil {
			return err
		}
	}

	if sql.readConn == nil {
		if err := sql.resetReadConn(); err != nil {
			return err
		}
	}

	q, err := sql.readConn.Prepare("SELECT DISTINCT id FROM shard")
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
		sql.shards[shardId], err = sql.readConn.Prepare(
			fmt.Sprintf("SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ?", shardId))
		if err != nil {
			return err
		}
	}
	err = q.Close()
	if err != nil {
		return err
	}

	q, err = sql.readConn.Prepare("SELECT version, id FROM shard")
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
		var version, shardId int64
		err = q.Scan(&version, &shardId)
		if err != nil {
			return err
		}
		sql.versionShard[version] = shardId
	}

	return q.Close()
}

func (sql *SqliteDb) WarmLeaves() error {
	start := time.Now()
	read, err := sql.getReadConn()
	if err != nil {
		return err
	}
	stmt, err := read.Prepare("SELECT version, sequence, bytes FROM leaf")
	if err != nil {
		return err
	}
	var (
		cnt, version, seq int64
		kz, vz            []byte
	)
	for {
		ok, err := stmt.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		cnt++
		err = stmt.Scan(&version, &seq, &vz)
		if err != nil {
			return err
		}
		if cnt%5_000_000 == 0 {
			sql.logger.Info().Msgf("warmed %s leaves", humanize.Comma(cnt))
		}
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	stmt, err = read.Prepare("SELECT key, value FROM latest")
	if err != nil {
		return err
	}
	for {
		ok, err := stmt.Step()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		cnt++
		err = stmt.Scan(&kz, &vz)
		if err != nil {
			return err
		}
		if cnt%5_000_000 == 0 {
			sql.logger.Info().Msgf("warmed %s leaves", humanize.Comma(cnt))
		}
	}

	sql.logger.Info().Msgf("warmed %s leaves in %s", humanize.Comma(cnt), time.Since(start))
	return stmt.Close()
}

func (sql *SqliteDb) getRightNode(node *Node) (*Node, error) {
	var err error
	if node.subtreeHeight == 1 || node.subtreeHeight == 2 {
		node.rightNode, err = sql.getLeaf(node.rightNodeKey)
		if err != nil {
			return nil, err
		}
		if node.rightNode != nil {
			return node.rightNode, nil
		} else {
			sql.metrics.QueryLeafMiss++
		}
	}

	node.rightNode, err = sql.Get(node.rightNodeKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get right node node_key=%s height=%d path=%s: %w",
			node.rightNodeKey, node.subtreeHeight, sql.opts.Path, err)
	}
	return node.rightNode, nil
}

func (sql *SqliteDb) getLeftNode(node *Node) (*Node, error) {
	var err error
	if node.subtreeHeight == 1 || node.subtreeHeight == 2 {
		node.leftNode, err = sql.getLeaf(node.leftNodeKey)
		if err != nil {
			return nil, err
		}
		if node.leftNode != nil {
			return node.leftNode, nil
		} else {
			sql.metrics.QueryLeafMiss++
		}
	}

	node.leftNode, err = sql.Get(node.leftNodeKey)
	if err != nil {
		return nil, err
	}
	return node.leftNode, err
}

func (sql *SqliteDb) Revert(version int) error {
	if err := sql.leafWrite.Exec("DELETE FROM leaf WHERE version > ?", version); err != nil {
		return err
	}
	if err := sql.leafWrite.Exec("DELETE FROM leaf_delete WHERE version > ?", version); err != nil {
		return err
	}
	if err := sql.treeWrite.Exec("DELETE FROM root WHERE version > ?", version); err != nil {
		return err
	}
	q, err := sql.treeWrite.Prepare("SELECT DISTINCT id FROM shard WHERE version > ?", version)
	if err != nil {
		return err
	}
	var shards []int
	for {
		hasRow, err := q.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}
		var shard int
		err = q.Scan(&shard)
		if err != nil {
			return err
		}
		shards = append(shards, shard)
	}
	if err = q.Close(); err != nil {
		return err
	}
	for _, shard := range shards {
		if err = sql.treeWrite.Exec(fmt.Sprintf("DROP TABLE IF EXISTS tree_%d", shard)); err != nil {
			return err
		}
	}
	if err = sql.treeWrite.Exec("DELETE FROM shard WHERE version > ?", version); err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) GetLatestLeaf(key []byte) ([]byte, error) {
	if sql.queryLatest == nil {
		var err error
		sql.queryLatest, err = sql.readConn.Prepare("SELECT value FROM changelog.latest WHERE key = ?")
		if err != nil {
			return nil, err
		}
	}
	defer sql.queryLatest.Reset()

	if err := sql.queryLatest.Bind(key); err != nil {
		return nil, err
	}
	hasRow, err := sql.queryLatest.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, nil
	}
	var val []byte
	err = sql.queryLatest.Scan(&val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (sql *SqliteDb) closeHangingIterators() error {
	for idx, stmt := range sql.iterators {
		sql.logger.Warn().Msgf("closing hanging iterator idx=%d", idx)
		if err := stmt.Close(); err != nil {
			return err
		}
		delete(sql.iterators, idx)
	}
	sql.itrIdx = 0
	return nil
}

func (sql *SqliteDb) getLeafIteratorQuery(start, end []byte, ascending, _ bool) (stmt *sqlite3.Stmt, idx int, err error) {
	var suffix string
	if ascending {
		suffix = "ASC"
	} else {
		suffix = "DESC"
	}

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, idx, err
	}

	sql.itrIdx++
	idx = sql.itrIdx

	switch {
	case start == nil && end == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf("SELECT key, value FROM changelog.latest ORDER BY key %s", suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(); err != nil {
			return nil, idx, err
		}
	case start == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf("SELECT key, value FROM changelog.latest WHERE key < ? ORDER BY key %s", suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(end); err != nil {
			return nil, idx, err
		}
	case end == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf("SELECT key, value FROM changelog.latest WHERE key >= ? ORDER BY key %s", suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(start); err != nil {
			return nil, idx, err
		}
	default:
		stmt, err = conn.Prepare(
			fmt.Sprintf("SELECT key, value FROM changelog.latest WHERE key >= ? AND key < ? ORDER BY key %s", suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(start, end); err != nil {
			return nil, idx, err
		}
	}

	sql.iterators[idx] = stmt
	return stmt, idx, err
}
