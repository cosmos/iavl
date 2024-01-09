package iavl

import (
	"fmt"
	"os"
	"strconv"
	"sync"
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

	metrics *metrics.TreeMetrics
	logger  zerolog.Logger

	pruningLock   sync.Mutex
	tableSwapLock sync.Mutex
}

func defaultSqliteDbOptions(opts SqliteDbOptions) SqliteDbOptions {
	if opts.Path == "" {
		opts.Path = "/tmp/iavl-v2"
	}
	if opts.MmapSize == 0 {
		opts.MmapSize = 8 * 1024 * 1024 * 1024
	}
	//if opts.ConnArgs == "" {
	//	opts.ConnArgs = "cache=shared"
	//}
	if opts.Metrics == nil {
		opts.Metrics = &metrics.TreeMetrics{}
	}
	if opts.WalSize == 0 {
		opts.WalSize = 1024 * 1024 * 100
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
	q, err := conn.Prepare("SELECT SUM(pgsize) FROM dbstat WHERE name = 'latest'")
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
	opts := defaultSqliteDbOptions(SqliteDbOptions{ConnArgs: "mode=memory&cache=shared"})
	return NewSqliteDb(pool, opts)
}

func NewSqliteDb(pool *NodePool, opts SqliteDbOptions) (*SqliteDb, error) {
	opts = defaultSqliteDbOptions(opts)
	logger := log.With().Str("module", "sqlite").Str("path", opts.Path).Logger()
	sql := &SqliteDb{
		shards:       &VersionRange{},
		shardQueries: make(map[int64]*sqlite3.Stmt),
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
CREATE TABLE leaf (version int, sequence int, bytes blob, orphaned bool);
CREATE TABLE leaf_delete (version int, sequence int, key blob, PRIMARY KEY (version, sequence));
CREATE TABLE leaf_orphan (version int, sequence int, at int);`)
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

	if err := q.Reset(); err != nil {
		return nil, err
	}
	if err := q.Bind(nodeKey.Version(), int(nodeKey.Sequence())); err != nil {
		return nil, err
	}
	hasRow, err := q.Step()
	if !hasRow {
		return nil, fmt.Errorf("node not found: %v; shard=%d", nodeKey, sql.shards.Find(nodeKey.Version()))
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
	sql.tableSwapLock.Lock()
	defer sql.tableSwapLock.Unlock()
	q, err := sql.getShardQuery(nodeKey.Version())
	if err != nil {
		return nil, err
	}
	return sql.getNode(nodeKey, q)
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

func (sql *SqliteDb) NextShard(version int64) error {
	sql.logger.Info().Msgf("creating shard %d", version)

	err := sql.treeWrite.Exec(fmt.Sprintf("CREATE TABLE tree_%d (version int, sequence int, bytes blob, orphaned bool);", version))
	if err != nil {
		return err
	}
	return sql.shards.Add(version)
}

func (sql *SqliteDb) IndexShard(shardId int64) error {
	err := sql.leafWrite.Exec(fmt.Sprintf("CREATE INDEX tree_%d_node_key_idx ON tree_%d (node_key);", shardId, shardId))
	return err
}

func (sql *SqliteDb) SaveRoot(version int64, node *Node, isCheckpoint bool) error {
	if node != nil {
		bz, err := node.Bytes()
		if err != nil {
			return err
		}
		return sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version, node_version, node_sequence, bytes, checkpoint) VALUES (?, ?, ?, ?, ?)",
			version, node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz, isCheckpoint)
	} else {
		// for an empty root a sentinel is saved
		return sql.treeWrite.Exec("INSERT OR REPLACE INTO root(version, checkpoint) VALUES (?, ?)", version, isCheckpoint)
	}
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
	// TODO this placement seems wrong?
	if err := sql.ResetShardQueries(); err != nil {
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

func (sql *SqliteDb) getShardQuery(version int64) (*sqlite3.Stmt, error) {
	v := sql.shards.Find(version)
	if v == -1 {
		return nil, fmt.Errorf("version %d is before the first shard", version)
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
	sql.logger.Debug().Msgf("added shard query: %s", sqlQuery)
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
			return err
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

// DeleteVersionsTo deletes all nodes with version up to and including the given version.  changelog entries with
// version > toVersion but prior to the next checkpoint are not removed.
func (sql *SqliteDb) DeleteVersionsTo(toVersion int64) error {
	start := time.Now()
	if err := sql.treeWrite.Exec("CREATE INDEX IF NOT EXISTS orphan_idx ON orphan (version, sequence);"); err != nil {
		return err
	}
	sql.logger.Info().Msgf("created orphan_idx in %s", time.Since(start))
	var checkpointVersion int64
	if err := sql.treeWrite.Begin(); err != nil {
		return err
	}
	for _, v := range sql.shards.versions {
		if v > toVersion {
			break
		}
		if err := sql.treeWrite.Exec(fmt.Sprintf(`
		DELETE FROM tree_%d
		WHERE ROWID IN (
		    SELECT t.ROWID
		    FROM orphan o
		    INNER JOIN tree_1 t ON t.version = o.version AND t.sequence = o.sequence
		);`, v)); err != nil {
			return err
		}

		//if err := sql.treeWrite.Exec(fmt.Sprintf("DELETE FROM tree_%d WHERE orphaned = true", v)); err != nil {
		//	return err
		//}
		if err := sql.treeWrite.Exec("DELETE FROM root WHERE version <= ?", v); err != nil {
			return err
		}
		checkpointVersion = v
	}

	if err := sql.leafWrite.Exec("DELETE FROM leaf WHERE version <= ? AND orphaned = true", checkpointVersion); err != nil {
		return err
	}

	// TODO: handle leaf_delete table

	if err := sql.treeWrite.Exec("DELETE FROM orphan WHERE version <= ?", checkpointVersion); err != nil {
		return err
	}

	if err := sql.treeWrite.Commit(); err != nil {
		return err
	}

	if err := sql.treeWrite.Exec("DROP INDEX orphan_idx;"); err != nil {
		return err
	}

	sql.logger.Info().Msgf("pruned versions <= %d in %s", toVersion, time.Since(start))

	return nil
}

func (sql *SqliteDb) DeleteVersionsTo_v2(toVersion int64) error {
	start := time.Now()
	//if err := sql.treeWrite.Exec("CREATE INDEX IF NOT EXISTS orphan_idx ON orphan (version, sequence);"); err != nil {
	//	return err
	//}
	//sql.logger.Info().Msgf("created orphan_idx in %s", time.Since(start))
	var checkpointVersion int64

	//if err := sql.treeWrite.Begin(); err != nil {
	//	return err
	//}

	conn, err := sqlite3.Open(sql.opts.treeConnectionString())
	if err != nil {
		return err
	}

	shardQuery, err := conn.Prepare("SELECT * FROM orphan WHERE version <= ?", toVersion)
	if err != nil {
		return err
	}

	since := time.Now()
	var (
		version  int64
		sequence int
	)
	deleteStmts := make(map[int64]*sqlite3.Stmt)
	for {
		hasRow, err := shardQuery.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}
		err = shardQuery.Scan(&version, &sequence)
		if err != nil {
			return err
		}
		cv := sql.shards.FindMemoized(version)
		if cv == -1 {
			return fmt.Errorf("checkpont for version %d not found", version)
		}
		stmt, ok := deleteStmts[cv]
		if !ok {
			sqlStmt := fmt.Sprintf("DELETE FROM tree_%d WHERE version = ? AND sequence = ?", cv)
			stmt, err = conn.Prepare(sqlStmt)
			if err != nil {
				return err
			}
			deleteStmts[cv] = stmt
		}
		if err := stmt.Exec(version, sequence); err != nil {
			return err
		}
		if err := stmt.Reset(); err != nil {
			return err
		}
		if cv > checkpointVersion {
			checkpointVersion = cv
		}
	}

	if err := shardQuery.Close(); err != nil {
		return err
	}

	for _, stmt := range deleteStmts {
		if err := stmt.Close(); err != nil {
			return err
		}
	}
	sql.logger.Info().Msgf("deleted orphaned nodes in %s", time.Since(since))

	//if err := sql.leafWrite.Exec("DELETE FROM leaf WHERE version <= ? AND orphaned = true", checkpointVersion); err != nil {
	//	return err
	//}
	//sql.logger.Info().Msgf("deleted orphaned leaves in %s", time.Since(since))
	//since = time.Now()
	//
	// TODO: handle leaf_delete table

	//if err := sql.treeWrite.Exec("DELETE FROM orphan WHERE version <= ?", checkpointVersion); err != nil {
	//	return err
	//}
	//sql.logger.Info().Msgf("deleted orphans in %s", time.Since(since))

	//if err := sql.treeWrite.Exec("DROP INDEX orphan_idx;"); err != nil {
	//	return err
	//}

	//if err := sql.treeWrite.Commit(); err != nil {
	//	return err
	//}

	sql.logger.Info().Msgf("pruned versions <= %d in %s", toVersion, time.Since(start))

	return nil
}

func (sql *SqliteDb) DeleteVersionsTo_V3(toVersion int64) error {
	// TODO try a new connection
	// review: https://www.sqlite.org/isolation.html
	var (
		start    = time.Now()
		versions []int64
	)
	conn, err := sqlite3.Open(sql.opts.treeConnectionString())
	if err != nil {
		return err
	}

	sql.pruningLock.Lock()
	defer sql.pruningLock.Unlock()

	if err := conn.Exec("CREATE INDEX IF NOT EXISTS orphan_idx ON orphan (version, sequence);"); err != nil {
		return err
	}
	sql.logger.Info().Msgf("created orphan_idx in %s", time.Since(start))

	since := time.Now()
	for _, v := range sql.shards.versions {
		if v > toVersion {
			break
		}
		versions = append(versions, v)
		if err := conn.Exec(fmt.Sprintf(
			"CREATE TABLE tree_%d_pruned (version int, sequence int, bytes blob, orphaned bool);", v)); err != nil {
			return fmt.Errorf("failed to create pruned table version=%d path=%s: %w", v, sql.opts.Path, err)
		}
		if err := conn.Exec(fmt.Sprintf(`
INSERT INTO tree_%d_pruned (version, sequence, bytes) 
SELECT t.version, t.sequence, t.bytes
FROM tree_%d t
LEFT JOIN orphan o ON o.version = t.version AND o.sequence = t.sequence
WHERE o.version IS NULL;`, v, v)); err != nil {
			return err
		}
		if err := conn.Exec(fmt.Sprintf("CREATE INDEX tree_%d_idx_%d ON tree_%d_pruned (version, sequence);",
			v, time.Now().UnixMilli(), v)); err != nil {
			return fmt.Errorf("failed to create index version=%d path=%s: %w", v, sql.opts.Path, err)
		}
	}
	sql.logger.Info().Msgf("copied nodes > %d in %s", toVersion, time.Since(since))
	sql.tableSwapLock.Lock()
	defer sql.tableSwapLock.Unlock()
	for _, v := range versions {
		if err := conn.Exec(fmt.Sprintf("DROP TABLE tree_%d", v)); err != nil {
			return fmt.Errorf("failed to drop table version=%d path=%s: %w", v, sql.opts.Path, err)
		}
		if err := conn.Exec(fmt.Sprintf("ALTER TABLE tree_%d_pruned RENAME TO tree_%d", v, v)); err != nil {
			return fmt.Errorf("failed to rename table version=%d path=%s: %w", v, sql.opts.Path, err)
		}
	}
	if err := conn.Exec("DROP INDEX orphan_idx;"); err != nil {
		return err
	}
	//sql.shards = &VersionRange{}
	//if err := sql.ResetShardQueries(); err != nil {
	//	return err
	//}

	// TODO need to synchronously back fill leaves since prune copy

	sql.logger.Info().Msgf("pruned versions <= %d in %s", toVersion, time.Since(start))

	return nil
}
