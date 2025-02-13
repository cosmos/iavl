package iavl

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"
)

const defaultSQLitePath = "/tmp/iavl-v2"

type SqliteDbOptions struct {
	Path       string
	Mode       int
	MmapSize   uint64
	WalSize    int
	CacheSize  int
	ConnArgs   string
	ShardTrees bool

	Logger  Logger
	Metrics metrics.Proxy

	walPages int
}

type SqliteDb struct {
	opts SqliteDbOptions

	pool *NodePool

	// 2 separate databases and 2 separate connections.  the underlying databases have different WAL policies
	// therefore separation is required.
	leafWrite *sqlite3.Conn
	treeWrite *sqlite3.Conn

	// for latest table queries
	itrIdx      int
	iterators   map[int]*sqlite3.Stmt
	queryLatest *sqlite3.Stmt

	readConn  *sqlite3.Conn
	queryLeaf *sqlite3.Stmt

	shards       *VersionRange
	shardQueries map[int64]*sqlite3.Stmt

	metrics metrics.Proxy
	logger  Logger
}

func defaultSqliteDbOptions(opts SqliteDbOptions) SqliteDbOptions {
	if opts.Path == "" {
		opts.Path = defaultSQLitePath
	}
	if opts.MmapSize == 0 {
		opts.MmapSize = 8 * 1024 * 1024 * 1024
	}
	if opts.WalSize == 0 {
		opts.WalSize = 1024 * 1024 * 100
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NilMetrics{}
	}
	opts.walPages = opts.WalSize / os.Getpagesize()

	if opts.Logger == nil {
		opts.Logger = NewNopLogger()
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

func (opts SqliteDbOptions) EstimateMmapSize() (uint64, error) {
	opts.Logger.Info("calculate mmap size")
	opts.Logger.Info(fmt.Sprintf("leaf connection string: %s", opts.leafConnectionString()))
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
		return 0, errors.New("no row")
	}
	var leafSize int64
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
	mmapSize := uint64(float64(leafSize) * 1.3)
	opts.Logger.Info(fmt.Sprintf("leaf mmap size: %s", humanize.Bytes(mmapSize)))

	return mmapSize, nil
}

func NewInMemorySqliteDb(pool *NodePool) (*SqliteDb, error) {
	opts := defaultSqliteDbOptions(SqliteDbOptions{ConnArgs: "mode=memory&cache=shared"})
	return NewSqliteDb(pool, opts)
}

func NewSqliteDb(pool *NodePool, opts SqliteDbOptions) (*SqliteDb, error) {
	opts = defaultSqliteDbOptions(opts)
	sql := &SqliteDb{
		shards:       &VersionRange{},
		shardQueries: make(map[int64]*sqlite3.Stmt),
		iterators:    make(map[int]*sqlite3.Stmt),
		opts:         opts,
		pool:         pool,
		metrics:      opts.Metrics,
		logger:       opts.Logger,
	}

	if !api.IsFileExistent(opts.Path) {
		err := os.MkdirAll(opts.Path, 0755)
		if err != nil {
			return nil, err
		}
	}

	if err := sql.resetWriteConn(); err != nil {
		return nil, err
	}

	if err := sql.init(); err != nil {
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
CREATE TABLE orphan (version int, sequence int, at int);
CREATE INDEX orphan_idx ON orphan (at);
CREATE TABLE root (
	version int, 
	node_version int, 
	node_sequence int, 
	bytes blob, 
	checkpoint bool, 
	PRIMARY KEY (version))`)
		if err != nil {
			return err
		}

		pageSize := os.Getpagesize()
		sql.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
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
CREATE TABLE leaf (version int, sequence int, bytes blob, orphaned bool);
CREATE TABLE leaf_delete (version int, sequence int, key blob, PRIMARY KEY (version, sequence));
CREATE TABLE leaf_orphan (version int, sequence int, at int);
CREATE INDEX leaf_orphan_idx ON leaf_orphan (at);`)
		if err != nil {
			return err
		}

		pageSize := os.Getpagesize()
		sql.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
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

func (sql *SqliteDb) resetWriteConn() (err error) {
	if sql.treeWrite != nil {
		err = sql.treeWrite.Close()
		if err != nil {
			return err
		}
	}
	sql.treeWrite, err = sqlite3.Open(sql.opts.treeConnectionString())
	if err != nil {
		return err
	}

	err = sql.treeWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}

	if err = sql.treeWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return err
	}

	sql.leafWrite, err = sqlite3.Open(sql.opts.leafConnectionString())
	if err != nil {
		return err
	}

	err = sql.leafWrite.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}

	if err = sql.leafWrite.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return err
	}

	return err
}

func (sql *SqliteDb) newReadConn() (*sqlite3.Conn, error) {
	conn, err := sqlite3.Open(sql.opts.treeConnectionString())
	if err != nil {
		return nil, err
	}
	err = conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", sql.opts.leafConnectionString()))
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

func (sql *SqliteDb) getLeaf(nodeKey NodeKey) (*Node, error) {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_get")
		sql.metrics.IncrCounter(1, metricsNamespace, "db_get_leaf")
	}()
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

	return node, nil
}

func (sql *SqliteDb) getNode(nodeKey NodeKey) (*Node, error) {
	start := time.Now()
	q, err := sql.getShardQuery(nodeKey.Version())
	if err != nil {
		return nil, err
	}
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_get")
		sql.metrics.IncrCounter(1, metricsNamespace, "db_get_branch")
	}()

	if err := q.Reset(); err != nil {
		return nil, err
	}
	if err := q.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}
	hasRow, err := q.Step()
	if !hasRow {
		return nil, fmt.Errorf("node not found: %v; shard=%d; path=%s",
			nodeKey, sql.shards.Find(nodeKey.Version()), sql.opts.Path)
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

	return node, nil
}

func (sql *SqliteDb) Close() error {
	for _, q := range sql.shardQueries {
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

func (sql *SqliteDb) nextShard(version int64) (int64, error) {
	if !sql.opts.ShardTrees {
		switch sql.shards.Len() {
		case 0:
			break
		case 1:
			return sql.shards.Last(), nil
		default:
			return -1, fmt.Errorf("sharding is disabled but found shards; shards=%v", sql.shards.versions)
		}
	}

	sql.logger.Info(fmt.Sprintf("creating shard %d", version))
	err := sql.treeWrite.Exec(fmt.Sprintf("CREATE TABLE tree_%d (version int, sequence int, bytes blob, orphaned bool);", version))
	if err != nil {
		return version, err
	}
	return version, sql.shards.Add(version)
}

func (sql *SqliteDb) SaveRoot(version int64, node *Node, isCheckpoint bool) error {
	if node != nil {
		bz, err := node.Bytes()
		if err != nil {
			return err
		}
		return sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version, node_version, node_sequence, bytes, checkpoint) VALUES (?, ?, ?, ?, ?)",
			version, node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz, isCheckpoint)
	}
	// for an empty root a sentinel is saved
	return sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version, checkpoint) VALUES (?, ?)", version, isCheckpoint)
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

	// if nodeBz is nil then a (valid) empty tree was saved, which a nil root represents
	var root *Node
	if nodeBz != nil {
		rootKey := NewNodeKey(nodeVersion, uint32(nodeSeq))
		root, err = MakeNode(sql.pool, rootKey, nodeBz)
		if err != nil {
			return nil, err
		}
	}

	if err := rootQuery.Close(); err != nil {
		return nil, err
	}
	if err := sql.ResetShardQueries(); err != nil {
		return nil, err
	}
	if err := conn.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

// lastCheckpoint fetches the last checkpoint version from the shard table previous to the loaded root's version.
// a return value of zero and nil error indicates no checkpoint was found.
func (sql *SqliteDb) lastCheckpoint(treeVersion int64) (checkpointVersion int64, err error) {
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
	err = rootQuery.Scan(&checkpointVersion)
	if err != nil {
		return 0, err
	}

	if err = rootQuery.Close(); err != nil {
		return 0, err
	}
	if err = conn.Close(); err != nil {
		return 0, err
	}
	return checkpointVersion, nil
}

func (sql *SqliteDb) loadCheckpointRange() (*VersionRange, error) {
	conn, err := sqlite3.Open(sql.opts.treeConnectionString())
	if err != nil {
		return nil, err
	}
	q, err := conn.Prepare("SELECT version FROM root WHERE checkpoint = true ORDER BY version")
	if err != nil {
		return nil, err
	}
	var version int64
	versionRange := &VersionRange{}
	for {
		hasRow, err := q.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		err = q.Scan(&version)
		if err != nil {
			return nil, err
		}
		if err = versionRange.Add(version); err != nil {
			return nil, err

		}
	}
	if err = q.Close(); err != nil {
		return nil, err
	}
	if err = conn.Close(); err != nil {
		return nil, err
	}
	return versionRange, nil
}

func (sql *SqliteDb) getShard(version int64) (int64, error) {
	if !sql.opts.ShardTrees {
		if sql.shards.Len() != 1 {
			return -1, fmt.Errorf("expected a single shard; path=%s", sql.opts.Path)
		}
		return sql.shards.Last(), nil
	}
	v := sql.shards.FindMemoized(version)
	if v == -1 {
		return -1, fmt.Errorf("version %d is after the first shard; shards=%v", version, sql.shards.versions)
	}
	return v, nil
}

func (sql *SqliteDb) getShardQuery(version int64) (*sqlite3.Stmt, error) {
	v, err := sql.getShard(version)
	if err != nil {
		return nil, err
	}

	if q, ok := sql.shardQueries[v]; ok {
		return q, nil
	}
	sqlQuery := fmt.Sprintf("SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ?", v)
	q, err := sql.readConn.Prepare(sqlQuery)
	if err != nil {
		return nil, err
	}
	sql.shardQueries[v] = q
	sql.logger.Debug(fmt.Sprintf("added shard query: %s", sqlQuery))
	return q, nil
}

func (sql *SqliteDb) ResetShardQueries() error {
	for k, q := range sql.shardQueries {
		err := q.Close()
		if err != nil {
			return err
		}
		delete(sql.shardQueries, k)
	}

	sql.shards = &VersionRange{}

	if sql.readConn == nil {
		if err := sql.resetReadConn(); err != nil {
			return err
		}
	}

	q, err := sql.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	if err != nil {
		return err
	}
	for {
		hasRow, err := q.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}
		var shard string
		err = q.Scan(&shard)
		if err != nil {
			return err
		}
		shardVersion, err := strconv.Atoi(shard[5:])
		if err != nil {
			return err
		}
		if err = sql.shards.Add(int64(shardVersion)); err != nil {
			return fmt.Errorf("failed to add shard path=%s: %w", sql.opts.Path, err)
		}
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
			sql.logger.Info(fmt.Sprintf("warmed %s leaves", humanize.Comma(cnt)))
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
			sql.logger.Info(fmt.Sprintf("warmed %s leaves", humanize.Comma(cnt)))
		}
	}

	sql.logger.Info(fmt.Sprintf("warmed %s leaves in %s", humanize.Comma(cnt), time.Since(start)))
	return stmt.Close()
}

func isLeafSeq(seq uint32) bool {
	return seq&(1<<31) != 0
}

func (sql *SqliteDb) getRightNode(node *Node) (*Node, error) {
	if node.isLeaf() {
		return nil, errors.New("leaf node has no children")
	}
	var err error
	if isLeafSeq(node.rightNodeKey.Sequence()) {
		node.rightNode, err = sql.getLeaf(node.rightNodeKey)
	} else {
		node.rightNode, err = sql.getNode(node.rightNodeKey)
	}
	if node.rightNode == nil {
		err = errors.New("not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get right node node_key=%s height=%d path=%s: %w",
			node.rightNodeKey, node.subtreeHeight, sql.opts.Path, err)
	}
	return node.rightNode, nil
}

func (sql *SqliteDb) getLeftNode(node *Node) (*Node, error) {
	if node.isLeaf() {
		return nil, errors.New("leaf node has no children")
	}
	var err error
	if isLeafSeq(node.leftNodeKey.Sequence()) {
		node.leftNode, err = sql.getLeaf(node.leftNodeKey)
	} else {
		node.leftNode, err = sql.getNode(node.leftNodeKey)
	}
	if node.leftNode == nil {
		err = errors.New("not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get right node node_key=%s height=%d path=%s: %w",
			node.leftNodeKey, node.subtreeHeight, sql.opts.Path, err)
	}
	return node.leftNode, nil
}

func (sql *SqliteDb) isSharded() (bool, error) {
	q, err := sql.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	if err != nil {
		return false, err
	}
	var cnt int
	for {
		hasRow, err := q.Step()
		if err != nil {
			return false, err
		}
		if !hasRow {
			break
		}
		cnt++
		if cnt > 1 {
			break
		}
	}
	return cnt > 1, q.Close()
}

func (sql *SqliteDb) Revert(version int) error {
	if err := sql.leafWrite.Exec("DELETE FROM leaf WHERE version > ?", version); err != nil {
		return err
	}
	if err := sql.leafWrite.Exec("DELETE FROM leaf_delete WHERE version > ?", version); err != nil {
		return err
	}
	if err := sql.leafWrite.Exec("DELETE FROM leaf_orphan WHERE at > ?", version); err != nil {
		return err
	}
	if err := sql.treeWrite.Exec("DELETE FROM root WHERE version > ?", version); err != nil {
		return err
	}
	if err := sql.treeWrite.Exec("DELETE FROM orphan WHERE at > ?", version); err != nil {
		return err
	}

	hasShards, err := sql.isSharded()
	if err != nil {
		return err
	}
	if hasShards {
		q, err := sql.treeWrite.Prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
		if err != nil {
			return err
		}
		var shards []string
		for {
			hasRow, err := q.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			var shard string
			err = q.Scan(&shard)
			if err != nil {
				return err
			}
			shardVersion, err := strconv.Atoi(shard[5:])
			if err != nil {
				return err
			}
			if shardVersion > version {
				shards = append(shards, shard)
			}
		}
		if err = q.Close(); err != nil {
			return err
		}
		for _, shard := range shards {
			if err = sql.treeWrite.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", shard)); err != nil {
				return err
			}
		}
	} else {

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
		sql.logger.Warn(fmt.Sprintf("closing hanging iterator idx=%d", idx))
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

func (sql *SqliteDb) replayChangelog(tree *Tree, toVersion int64, targetHash []byte) error {
	var (
		version     int
		lastVersion int
		sequence    int
		bz          []byte
		key         []byte
		count       int64
		start       = time.Now()
		since       = time.Now()
		logPath     = []interface{}{"path", sql.opts.Path}
	)
	tree.isReplaying = true
	defer func() {
		tree.isReplaying = false
	}()

	sql.opts.Logger.Info("ensure leaf_delete_index exists...", logPath...)
	if err := sql.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_delete_idx ON leaf_delete (version, sequence)"); err != nil {
		return err
	}
	sql.opts.Logger.Info("...done", logPath...)
	sql.opts.Logger.Info(fmt.Sprintf("replaying changelog from=%d to=%d", tree.version, toVersion), logPath...)
	conn, err := sql.getReadConn()
	if err != nil {
		return err
	}
	q, err := conn.Prepare(`SELECT * FROM (
		SELECT version, sequence, bytes, null AS key
	FROM leaf WHERE version > ? AND version <= ?
	UNION
	SELECT version, sequence, null as bytes, key
	FROM leaf_delete WHERE version > ? AND version <= ?
	) as ops
	ORDER BY version, sequence`)
	if err != nil {
		return err
	}
	if err = q.Bind(tree.version, toVersion, tree.version, toVersion); err != nil {
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
		count++
		if err = q.Scan(&version, &sequence, &bz, &key); err != nil {
			return err
		}
		if version-1 != lastVersion {
			tree.leaves, tree.branches, tree.leafOrphans, tree.deletes = nil, nil, nil, nil
			tree.version = int64(version - 1)
			tree.resetSequences()
			lastVersion = version - 1
		}
		if bz != nil {
			nk := NewNodeKey(0, 0)
			node, err := MakeNode(tree.pool, nk, bz)
			if err != nil {
				return err
			}
			if _, err = tree.Set(node.key, node.hash); err != nil {
				return err
			}
			if sequence != int(tree.leafSequence) {
				return fmt.Errorf("sequence mismatch version=%d; expected %d got %d; path=%s",
					version, sequence, tree.leafSequence, sql.opts.Path)
			}
		} else {
			if _, _, err = tree.Remove(key); err != nil {
				return err
			}
			deleteSequence := tree.deletes[len(tree.deletes)-1].deleteKey.Sequence()
			if sequence != int(deleteSequence) {
				return fmt.Errorf("sequence delete mismatch; version=%d expected %d got %d; path=%s",
					version, sequence, tree.leafSequence, sql.opts.Path)
			}
		}
		if count%250_000 == 0 {
			sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s node/s=%s",
				version, humanize.Comma(count), humanize.Comma(int64(250_000/time.Since(since).Seconds()))), logPath)
			since = time.Now()
		}
	}
	rootHash := tree.computeHash()
	if !bytes.Equal(targetHash, rootHash) {
		return fmt.Errorf("root hash mismatch; expected %x got %x", targetHash, rootHash)
	}
	tree.leaves, tree.branches, tree.leafOrphans, tree.deletes = nil, nil, nil, nil
	tree.resetSequences()
	tree.version = toVersion
	sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s dur=%s root=%v",
		tree.version, humanize.Comma(count), time.Since(start).Round(time.Millisecond), tree.root), logPath)
	return q.Close()
}

func (sql *SqliteDb) WriteLatestLeaves(tree *Tree) (err error) {
	var (
		since        = time.Now()
		batchSize    = 200_000
		count        = 0
		step         func(node *Node) error
		logPath      = []string{"path", sql.opts.Path}
		latestInsert *sqlite3.Stmt
	)
	prepare := func() error {
		latestInsert, err = sql.leafWrite.Prepare("INSERT INTO latest (key, value) VALUES (?, ?)")
		if err != nil {
			return err
		}
		if err = sql.leafWrite.Begin(); err != nil {
			return err
		}
		return nil
	}

	flush := func() error {
		if err = sql.leafWrite.Commit(); err != nil {
			return err
		}
		if err = latestInsert.Close(); err != nil {
			return err
		}
		var rate string
		if time.Since(since).Seconds() > 0 {
			rate = humanize.Comma(int64(float64(batchSize) / time.Since(since).Seconds()))
		} else {
			rate = "n/a"
		}
		sql.logger.Info(fmt.Sprintf("latest flush; count=%s dur=%s wr/s=%s",
			humanize.Comma(int64(count)),
			time.Since(since).Round(time.Millisecond),
			rate,
		), logPath)
		since = time.Now()
		return nil
	}

	maybeFlush := func() error {
		count++
		if count%batchSize == 0 {
			err = flush()
			if err != nil {
				return err
			}
			return prepare()
		}
		return nil
	}

	if err = prepare(); err != nil {
		return err
	}

	step = func(node *Node) error {
		if node.isLeaf() {
			err := latestInsert.Exec(node.key, node.value)
			if err != nil {
				return err
			}
			return maybeFlush()
		}
		if err = step(node.left(tree)); err != nil {
			return err
		}
		if err = step(node.right(tree)); err != nil {
			return err
		}
		return nil
	}

	err = step(tree.root)
	if err != nil {
		return err
	}
	err = flush()
	if err != nil {
		return err
	}

	return latestInsert.Close()
}

func (sql *SqliteDb) Logger() Logger {
	return sql.logger
}
