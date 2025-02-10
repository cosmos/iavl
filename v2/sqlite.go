package iavl

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"
	"github.com/rs/zerolog"
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
	Readonly   bool
	Metrics    metrics.Proxy

	walPages int
}

type SqliteDb struct {
	opts SqliteDbOptions

	pool *NodePool

	// read connection
	// life cycle: application
	hotConnectionFactory *hotConnectionFactory

	// write connection
	// life cycle: checkpoint
	writeConn *sqlite3.Conn

	// for latest table queries
	itrIdx    int
	iterators map[int]*sqlite3.Stmt

	shards *VersionRange

	metrics metrics.Proxy
	logger  zerolog.Logger

	pruning bool
	pruneCh chan *pruneResult
}

func defaultSqliteDbOptions(opts SqliteDbOptions) SqliteDbOptions {
	if opts.Path == "" {
		opts.Path = defaultSQLitePath
	}
	if opts.MmapSize == 0 {
		opts.MmapSize = 8 * 1024 * 1024 * 1024
	}
	if opts.WalSize == 0 {
		opts.WalSize = 1024 * 1024 * 250
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NilMetrics{}
	}
	opts.walPages = opts.WalSize / os.Getpagesize()
	return opts
}

func (opts SqliteDbOptions) connArgs() string {
	if opts.ConnArgs == "" {
		return ""
	}
	return fmt.Sprintf("?%s", opts.ConnArgs)
}

func (opts SqliteDbOptions) rootConnectString() string {
	return fmt.Sprintf("file:%s/root.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) hubConnectString() string {
	return fmt.Sprintf("file:%s/hub.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) shardPath(version int64) string {
	return fmt.Sprintf("%s/tree_%013d", opts.Path, version)
}

func (opts SqliteDbOptions) treeConnectionString(version int64) string {
	return fmt.Sprintf("file:%s.sqlite%s", opts.shardPath(version), opts.connArgs())
}

func (opts SqliteDbOptions) shortPath() string {
	return filepath.Base(opts.Path)
}

func (opts SqliteDbOptions) EstimateMmapSize() (uint64, error) {
	logger := log.With().Str("path", opts.Path).Logger()
	logger.Info().Msgf("calculate mmap size")
	logger.Info().Msgf("tree connection string: %s", opts.treeConnectionString(0))
	conn, err := sqlite3.Open(opts.treeConnectionString(0))
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
	logger.Info().Msgf("leaf mmap size: %s", humanize.Bytes(mmapSize))

	return mmapSize, nil
}

//	TODO delete MemoryDb support
//
// NewInMemorySqliteDb probably needs deleting now that the file system is a source of truth for shards.
// Otherwise shard indexing can be pushed into root.db
func NewInMemorySqliteDb(pool *NodePool) (*SqliteDb, error) {
	opts := defaultSqliteDbOptions(SqliteDbOptions{ConnArgs: "mode=memory&cache=shared"})
	return NewSqliteDb(pool, opts)
}

func NewSqliteDb(pool *NodePool, opts SqliteDbOptions) (*SqliteDb, error) {
	opts = defaultSqliteDbOptions(opts)
	logger := log.With().Str("path", filepath.Base(opts.Path)).Logger()

	if !api.IsFileExistent(opts.Path) {
		err := os.MkdirAll(opts.Path, 0o755)
		if err != nil {
			return nil, err
		}
	}
	hubConn, err := sqlite3.Open(opts.hubConnectString())
	if err != nil {
		return nil, err
	}
	err = hubConn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", opts.MmapSize))
	if err != nil {
		return nil, err
	}
	sql := &SqliteDb{
		shards:               &VersionRange{},
		hotConnectionFactory: newHotConnectionFactory(hubConn, opts),
		iterators:            make(map[int]*sqlite3.Stmt),
		opts:                 opts,
		metrics:              opts.Metrics,
		pool:                 pool,
		logger:               logger,
		pruneCh:              make(chan *pruneResult),
	}

	if err := sql.ensureRootDb(); err != nil {
		return nil, err
	}
	if err := sql.loadShards(); err != nil {
		return nil, err
	}

	return sql, nil
}

func (sql *SqliteDb) rootConnection() (*sqlite3.Conn, error) {
	return sqlite3.Open(sql.opts.rootConnectString())
}

// readConnectionFactory returns a connection factory that memoizes connections
// not thread safe
func (sql *SqliteDb) readConnectionFactory() connectionFactory {
	return newReadConnectionFactory(sql)
}

func (sql *SqliteDb) newShardReadConnection(shardID int64) (*sqliteConnection, error) {
	conn, err := sqlite3.Open(sql.opts.treeConnectionString(shardID))
	if err != nil {
		return nil, err
	}
	s := &sqliteConnection{shardID: shardID, conn: conn}
	err = s.conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d;", sql.opts.MmapSize))
	if err != nil {
		return nil, err
	}
	s.queryLeaf, err = conn.Prepare("SELECT bytes FROM leaf WHERE version = ? AND sequence = ?")
	if err != nil {
		return nil, err
	}
	s.queryBranch, err = conn.Prepare("SELECT bytes FROM tree WHERE version = ? AND sequence = ?")
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (sql *SqliteDb) ensureRootDb() (topErr error) {
	conn, err := sql.rootConnection()
	if err != nil {
		return err
	}
	q, err := conn.Prepare("SELECT name from sqlite_master WHERE type='table' AND name='root'")
	if err != nil {
		return err
	}
	defer func() {
		if err := q.Close(); err != nil {
			topErr = errors.Join(topErr, err)
			return
		}
		if err := conn.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()

	hasRow, err := q.Step()
	if err != nil {
		return err
	}
	if hasRow {
		return nil
	}
	pageSize := os.Getpagesize()
	err = conn.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
	if err != nil {
		return err
	}
	err = conn.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}
	err = conn.Exec(`
CREATE TABLE latest (key blob, value blob, PRIMARY KEY (key));
CREATE TABLE root (
	version int, 
	node_version int, 
	node_sequence int, 
	bytes blob, 
	checkpoint bool, 
	pruned bool,
	PRIMARY KEY (version));`)
	return err
}

// sharding

var errShardExists = errors.New("shard already exists")

func (sql *SqliteDb) createTreeShardDb(version int64, createIndex bool) (topErr error) {
	dbPath := sql.opts.shardPath(version) + ".sqlite"
	if api.IsFileExistent(dbPath) {
		return errShardExists
	}

	conn, err := sqlite3.Open(sql.opts.treeConnectionString(version))
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()
	err = conn.Exec(`
CREATE TABLE tree (version int, sequence int, bytes blob, orphaned int);
CREATE TABLE orphan (version int, sequence int, at int);
CREATE TABLE leaf (version int, sequence int, bytes blob, orphaned int);
CREATE TABLE leaf_delete (version int, sequence int, key blob, PRIMARY KEY (version, sequence));
CREATE TABLE leaf_orphan (version int, sequence int, at int);
CREATE TABLE checkpoints (version int, prune_to int, branch_orphans int, leaf_orphans int, PRIMARY KEY (version))
`)
	if err != nil {
		return err
	}
	if createIndex {
		err = conn.Exec("CREATE UNIQUE INDEX leaf_idx ON leaf (version, sequence)")
		if err != nil {
			return err
		}
		err = conn.Exec("CREATE INDEX tree_idx ON tree (version, sequence)")
		if err != nil {
			return err
		}
	}

	pageSize := os.Getpagesize()
	err = conn.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
	if err != nil {
		return err
	}
	err = conn.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) createShard(version int64, createIndex bool) error {
	if err := sql.createTreeShardDb(version, createIndex); err != nil {
		return err
	}
	if err := sql.shards.Add(version); err != nil {
		return err
	}
	if err := sql.hotConnectionFactory.addShard(version); err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) lockShard(version int64) error {
	lockFile := sql.opts.shardPath(version) + ".lock"
	if api.IsFileExistent(lockFile) {
		return nil
	}
	f, err := os.Create(lockFile)
	if err != nil {
		return err
	}
	return f.Close()
}

func (sql *SqliteDb) unlockShard(version int64) error {
	lockFile := sql.opts.shardPath(version) + ".lock"
	return os.Remove(lockFile)
}

func (sql *SqliteDb) isShardLocked(version int64) bool {
	lockFile := sql.opts.shardPath(version) + ".lock"
	return api.IsFileExistent(lockFile)
}

func (sql *SqliteDb) newWriteConnection(version int64) (*sqlite3.Conn, error) {
	shardID := sql.shards.FindShard(version)
	if shardID == -1 {
		return nil, fmt.Errorf("shard not found version=%d shards=%v", version, sql.shards.versions)
	}
	conn, err := sqlite3.Open(sql.opts.treeConnectionString(shardID))
	if err != nil {
		return nil, err
	}
	err = conn.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	if err = conn.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return nil, err
	}
	return conn, nil
}

func (sql *SqliteDb) ensureWriteConnection() error {
	if sql.writeConn != nil {
		return nil
	}
	return sql.resetWriteConnection()
}

func (sql *SqliteDb) resetWriteConnection() error {
	if sql.writeConn != nil {
		if err := sql.writeConn.Close(); err != nil {
			return err
		}
	}
	conn, err := sql.newWriteConnection(sql.shards.Last())
	if err != nil {
		return err
	}
	sql.writeConn = conn
	return err
}

func (sql *SqliteDb) listShards() ([]int64, error) {
	files, err := os.ReadDir(sql.opts.Path)
	if err != nil {
		return nil, err
	}
	var versions []int64
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if match, _ := filepath.Match("tree_*.sqlite", file.Name()); match {
			shardVersion, err := strconv.Atoi(file.Name()[5 : len(file.Name())-7])
			if err != nil {
				return nil, err
			}
			if sql.isShardLocked(int64(shardVersion)) {
				sql.logger.Warn().Msgf("shard %d is locked and possibly orphaned", shardVersion)
				continue
			}
			versions = append(versions, int64(shardVersion))
		}
	}
	return versions, nil
}

func (sql *SqliteDb) updateShard(version, pruneTo, branchOrphans, leafOrphans int64) error {
	return sql.writeConn.Exec(
		"INSERT OR REPLACE INTO checkpoints (version, prune_to, branch_orphans, leaf_orphans) VALUES (?, ?, ?, ?)",
		version, pruneTo, branchOrphans, leafOrphans,
	)
}

func (sql *SqliteDb) getCheckpointCounts(version int64) (pruneTo, branches int64, leaves int64, topErr error) {
	shardID := sql.shards.FindShard(version)
	_, err := sql.hotConnectionFactory.make(shardID)
	if err != nil {
		return 0, 0, 0, err
	}
	conn := sql.hotConnectionFactory.main
	q, err := conn.Prepare(
		fmt.Sprintf("SELECT prune_to, branch_orphans, leaf_orphans FROM shard_%d.checkpoints WHERE version = ?", shardID))
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err := q.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()
	if err := q.Bind(version); err != nil {
		return 0, 0, 0, err
	}
	ok, err := q.Step()
	if err != nil {
		return 0, 0, 0, err
	}
	if !ok {
		return 0, 0, 0, fmt.Errorf("checkpoint not found: %d shard=%d path=%s", version, shardID, filepath.Base(sql.opts.Path))
	}
	if err := q.Scan(&pruneTo, &branches, &leaves); err != nil {
		return 0, 0, 0, err
	}
	return pruneTo, branches, leaves, nil
}

func (sql *SqliteDb) loadShards() error {
	shardVersions, err := sql.listShards()
	if err != nil {
		return err
	}

	if len(shardVersions) == 0 {
		return nil
	}

	sql.shards = &VersionRange{}
	for _, shardID := range shardVersions {
		if err = sql.hotConnectionFactory.addShard(shardID); err != nil {
			return err
		}
		if err = sql.shards.Add(shardID); err != nil {
			return err
		}
	}
	return nil
}

func (sql *SqliteDb) Close() error {
	if sql.pruning {
		sql.logger.Warn().Msg("pruning in progress, waiting...")
		start := time.Now()
		for sql.pruning {
			if time.Since(start) > 30*time.Second {
				return fmt.Errorf("pruning did not complete in 30s")
			}
			if err := sql.checkPruning(); err != nil {
				return err
			}
		}
	}
	if sql.writeConn != nil {
		if err := sql.writeConn.Close(); err != nil {
			return err
		}
	}
	return sql.hotConnectionFactory.close()
}

// read API

func (sql *SqliteDb) getLeaf(nodeKey NodeKey, cf connectionFactory) (node *Node, topErr error) {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_get")
		sql.metrics.IncrCounter(1, metricsNamespace, "db_get_leaf")
	}()
	shard := sql.shards.FindShard(nodeKey.Version())
	conn, err := cf.make(shard)
	if err != nil {
		return nil, err
	}

	if err = conn.queryLeaf.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}
	defer func() {
		if err := conn.queryLeaf.Reset(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()
	hasRow, err := conn.queryLeaf.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, nil
	}

	var nodeBz sqlite3.RawBytes
	err = conn.queryLeaf.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}
	node, err = MakeNode(sql.pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (sql *SqliteDb) getBranch(nodeKey NodeKey, cf connectionFactory) (node *Node, topErr error) {
	start := time.Now()
	defer func() {
		defer func() {
			sql.metrics.MeasureSince(start, metricsNamespace, "db_get")
			sql.metrics.IncrCounter(1, metricsNamespace, "db_get_branch")
		}()
	}()
	shard := sql.shards.FindShard(nodeKey.Version())
	if cf == nil {
		return nil, fmt.Errorf("connection factory is nil")
	}
	conn, err := cf.make(shard)
	if err != nil {
		return nil, err
	}

	if err := conn.queryBranch.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}
	defer func() {
		if err := conn.queryBranch.Reset(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()

	hasRow, err := conn.queryBranch.Step()
	if !hasRow {
		shardAt := sql.shards.FindShard(nodeKey.Version())
		return nil, fmt.Errorf("node not found: %v; shard=%d; path=%s", nodeKey, shardAt, sql.opts.Path)
	}
	if err != nil {
		return nil, err
	}
	var nodeBz sqlite3.RawBytes
	err = conn.queryBranch.Scan(&nodeBz)
	if err != nil {
		return nil, err
	}
	node, err = MakeNode(sql.pool, nodeKey, nodeBz)
	if err != nil {
		return nil, err
	}
	err = conn.queryBranch.Reset()
	if err != nil {
		return nil, err
	}

	return node, nil
}

func isLeafSeq(seq uint32) bool {
	return seq&(1<<31) != 0
}

func (sql *SqliteDb) getRightNode(node *Node, cf connectionFactory) (*Node, error) {
	var err error
	if isLeafSeq(node.rightNodeKey.Sequence()) {
		node.rightNode, err = sql.getLeaf(node.rightNodeKey, cf)
		if err != nil {
			return nil, err
		}
		if node.rightNode != nil {
			return node.rightNode, nil
		}
		sql.metrics.IncrCounter(1, metricsNamespace, "leaf_miss")
	}

	node.rightNode, err = sql.getBranch(node.rightNodeKey, cf)
	if err != nil {
		return nil, fmt.Errorf("failed to get right node node_key=%s height=%d path=%s: %w",
			node.rightNodeKey, node.subtreeHeight, sql.opts.Path, err)
	}
	return node.rightNode, nil
}

func (sql *SqliteDb) getLeftNode(node *Node, cf connectionFactory) (*Node, error) {
	var err error
	if isLeafSeq(node.leftNodeKey.Sequence()) {
		node.leftNode, err = sql.getLeaf(node.leftNodeKey, cf)
		if err != nil {
			return nil, err
		}
		if node.leftNode != nil {
			return node.leftNode, nil
		}
		sql.metrics.IncrCounter(1, metricsNamespace, "leaf_miss")
	}

	node.leftNode, err = sql.getBranch(node.leftNodeKey, cf)
	if err != nil {
		return nil, err
	}
	return node.leftNode, err
}

func (sql *SqliteDb) SaveRoot(version int64, node *Node, isCheckpoint bool) (topErr error) {
	conn, err := sql.rootConnection()
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()
	if node != nil {
		bz, err := node.Bytes()
		if err != nil {
			return err
		}
		return conn.Exec(
			"INSERT OR REPLACE INTO root(version, node_version, node_sequence, bytes, checkpoint, pruned) VALUES (?, ?, ?, ?, ?, false)",
			version, node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz, isCheckpoint)
	}
	// for an empty root a sentinel is saved
	return conn.Exec("INSERT OR REPLACE INTO root(version, checkpoint, pruned) VALUES (?, ?, false)", version, isCheckpoint)
}

func (sql *SqliteDb) LoadRoot(version int64) (root *Node, topErr error) {
	conn, err := sql.rootConnection()
	if err != nil {
		return nil, err
	}
	rootQuery, err := conn.Prepare("SELECT node_version, node_sequence, bytes FROM root WHERE version = ?", version)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rootQuery.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
		if err := conn.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()

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
	if nodeBz != nil {
		rootKey := NewNodeKey(nodeVersion, uint32(nodeSeq))
		root, err = MakeNode(sql.pool, rootKey, nodeBz)
		if err != nil {
			return nil, err
		}
	}

	return root, nil
}

func (sql *SqliteDb) loadCheckpointRange() (versionRange *VersionRange, topErr error) {
	conn, err := sql.rootConnection()
	if err != nil {
		return nil, err
	}
	q, err := conn.Prepare(
		"SELECT version FROM root WHERE checkpoint = true AND pruned = false ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := q.Close(); err != nil {
			topErr = errors.Join(err, err)
		}
		if err := conn.Close(); err != nil {
			topErr = errors.Join(err, err)
		}
	}()

	var version int64
	versionRange = &VersionRange{}
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
	return versionRange, nil
}

func (sql *SqliteDb) WarmLeaves() error {
	start := time.Now()
	hotConn, err := sql.hotConnectionFactory.make(sql.shards.Last())
	if err != nil {
		return err
	}
	read := hotConn.conn
	stmt, err := read.Prepare("SELECT version, sequence, bytes FROM leaf")
	if err != nil {
		return err
	}
	var (
		cnt, version, seq int64
		vz                []byte
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

	sql.logger.Info().Msgf("warmed %s leaves in %s", humanize.Comma(cnt), time.Since(start))
	return stmt.Close()
}

func (sql *SqliteDb) Revert(version int) error {
	panic("SqliteDb.Revert not implemented")
}

func (sql *SqliteDb) GetLatestLeaf(key []byte) (val []byte, topErr error) {
	// TODO: if used there ought to be a sustained connection for the root db used here
	conn, err := sql.rootConnection()
	if err != nil {
		return nil, err
	}
	q, err := conn.Prepare("SELECT value FROM latest WHERE key = ?")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := q.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
		if err := conn.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()

	if err := q.Bind(key); err != nil {
		return nil, err
	}
	hasRow, err := q.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, nil
	}

	err = q.Scan(&val)
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

func (sql *SqliteDb) getLeafIteratorQuery(start, end []byte, ascending, _ bool) (stmt *sqlite3.Stmt, idx int, topErr error) {
	var suffix string
	if ascending {
		suffix = "ASC"
	} else {
		suffix = "DESC"
	}

	// TODO: if used there ought to be a sustained connection for the root db here
	conn, err := sql.rootConnection()
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if stmt != nil {
			if err := stmt.Close(); err != nil {
				topErr = errors.Join(topErr, err)
			}
		}
		if err := conn.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()

	sql.itrIdx++
	idx = sql.itrIdx

	switch {
	case start == nil && end == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf("SELECT key, value FROM latest ORDER BY key %s", suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(); err != nil {
			return nil, idx, err
		}
	case start == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf("SELECT key, value FROM latest WHERE key < ? ORDER BY key %s", suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(end); err != nil {
			return nil, idx, err
		}
	case end == nil:
		stmt, err = conn.Prepare(
			fmt.Sprintf("SELECT key, value FROM latest WHERE key >= ? ORDER BY key %s", suffix))
		if err != nil {
			return nil, idx, err
		}
		if err = stmt.Bind(start); err != nil {
			return nil, idx, err
		}
	default:
		stmt, err = conn.Prepare(
			fmt.Sprintf("SELECT key, value FROM latest WHERE key >= ? AND key < ? ORDER BY key %s", suffix))
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

func (sql *SqliteDb) replayChangelog(tree *Tree, toVersion int64, targetHash []byte) (topErr error) {
	var (
		version     int
		lastVersion int
		sequence    int
		bz          []byte
		key         []byte
		count       int64
		start       = time.Now()
		lg          = log.With().Str("path", sql.opts.Path).Logger()
		since       = time.Now()
	)
	writeConn, err := sql.newWriteConnection(toVersion)
	if err != nil {
		return err
	}
	defer func() {
		if err := writeConn.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()
	versions, err := sql.listShards()
	if err != nil {
		return err
	}
	lg.Info().Msgf("replaying changelog from=%d to=%d", tree.version, toVersion)
	cf := sql.readConnectionFactory()
	for _, v := range versions {
		if v > toVersion {
			break
		}
		if err := writeConn.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_delete_idx ON leaf_delete (version, sequence)"); err != nil {
			return err
		}
		reader, err := cf.make(v)
		if err != nil {
			return err
		}
		q, err := reader.conn.Prepare(`SELECT * FROM (
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
			if version != lastVersion {
				tree.leaves, tree.leafOrphans, tree.deletes = nil, nil, nil
				tree.version = int64(version - 1)
				tree.stagedVersion = int64(version)
				tree.resetSequence()
				lastVersion = version
			}
			if bz != nil {
				nk := NewNodeKey(0, 0)
				node, err := MakeNode(tree.pool, nk, bz)
				if err != nil {
					return err
				}
				tree.replayHash = node.hash
				if _, err = tree.Set(node.key, node.value); err != nil {
					return err
				}
				tree.replayHash = nil
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
				lg.Info().Msgf("replayed changelog to version=%d count=%s node/s=%s",
					version, humanize.Comma(count), humanize.Comma(int64(250_000/time.Since(since).Seconds())))
				since = time.Now()
			}
		}
		if err = q.Close(); err != nil {
			return err
		}
	}
	rootHash := tree.computeHash()
	if !bytes.Equal(targetHash, rootHash) {
		return fmt.Errorf("root hash mismatch; expected %x got %x; path=%s count=%d",
			targetHash, rootHash, sql.opts.Path, count)
	}

	tree.leaves, tree.leafOrphans, tree.deletes = nil, nil, nil
	tree.resetSequence()
	tree.version = toVersion
	tree.stagedVersion = toVersion + 1
	tree.root = tree.stagedRoot

	lg.Info().Msgf("replayed changelog to version=%d count=%s dur=%s root=%v hash=%x",
		tree.version, humanize.Comma(count), time.Since(start).Round(time.Millisecond), tree.root, rootHash)
	return cf.close()
}

func (sql *SqliteDb) WriteLatestLeaves(tree *Tree) (topErr error) {
	var (
		since        = time.Now()
		batchSize    = 200_000
		count        = 0
		step         func(node *Node) error
		lg           = log.With().Str("path", sql.opts.Path).Logger()
		latestInsert *sqlite3.Stmt
	)
	conn, err := sql.rootConnection()
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			topErr = errors.Join(topErr, err)
		}
	}()

	prepare := func() error {
		latestInsert, err = conn.Prepare("INSERT INTO latest (key, value) VALUES (?, ?)")
		if err != nil {
			return err
		}
		if err = conn.Begin(); err != nil {
			return err
		}
		return nil
	}

	flush := func() error {
		if err = conn.Commit(); err != nil {
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
		lg.Info().Msgf("latest flush; count=%s dur=%s wr/s=%s",
			humanize.Comma(int64(count)),
			time.Since(since).Round(time.Millisecond),
			rate,
		)
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

	cf := tree.sql.readConnectionFactory()
	step = func(node *Node) error {
		if node.isLeaf() {
			err := latestInsert.Exec(node.key, node.value)
			if err != nil {
				return err
			}
			return maybeFlush()
		}
		if err = step(node.left(tree.sql, cf)); err != nil {
			return err
		}
		if err = step(node.right(tree.sql, cf)); err != nil {
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
