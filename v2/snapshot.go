package iavl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"
	"github.com/kocubinski/costor-api/logz"
)

type sqliteSnapshot struct {
	ctx context.Context

	snapshotInsert *sqlite3.Stmt

	sql        *SqliteDb
	leafInsert *sqlite3.Stmt
	treeInsert *sqlite3.Stmt

	// if set will flush nodes to a tree & leaf tables as well as a snapshot table during import
	writeTree bool

	lastWrite time.Time
	ordinal   int
	batchSize int
	version   int64
	getLeft   func(*Node) *Node
	getRight  func(*Node) *Node
	log       Logger
}

func (sql *SqliteDb) Snapshot(ctx context.Context, tree *Tree) error {
	version := tree.version
	err := sql.leafWrite.Exec(
		fmt.Sprintf("CREATE TABLE snapshot_%d (ordinal int, version int, sequence int, bytes blob);", version))
	if err != nil {
		return err
	}

	snapshot := &sqliteSnapshot{
		ctx:       ctx,
		sql:       sql,
		batchSize: 200_000,
		version:   version,
		log:       sql.logger,
		getLeft: func(node *Node) *Node {
			return node.left(tree)
		},
		getRight: func(node *Node) *Node {
			return node.right(tree)
		},
	}
	if err = snapshot.prepareWrite(); err != nil {
		return err
	}
	if err = snapshot.writeStep(tree.root); err != nil {
		return err
	}
	if err = snapshot.flush(); err != nil {
		return err
	}
	sql.logger.Info(fmt.Sprintf("creating index on snapshot_%d", version), "path", sql.opts.Path)
	err = sql.leafWrite.Exec(fmt.Sprintf("CREATE INDEX snapshot_%d_idx ON snapshot_%d (ordinal);", version, version))
	return err
}

type SnapshotOptions struct {
	StoreLeafValues   bool
	WriteCheckpoint   bool
	DontWriteSnapshot bool
	TraverseOrder     TraverseOrderType
}

func NewIngestSnapshotConnection(snapshotDbPath string) (*sqlite3.Conn, error) {
	newDb := !api.IsFileExistent(snapshotDbPath)

	conn, err := sqlite3.Open(fmt.Sprintf("file:%s", snapshotDbPath))
	if err != nil {
		return nil, err
	}
	pageSize := os.Getpagesize()
	if newDb {
		err = conn.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return nil, err
		}

		err = conn.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			return nil, err
		}
	}
	err = conn.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return nil, err
	}
	walSize := 1024 * 1024 * 1024
	if err = conn.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", walSize/pageSize)); err != nil {
		return nil, err
	}
	return conn, err
}

func IngestSnapshot(conn *sqlite3.Conn, prefix string, version int64, nextFn func() (*SnapshotNode, error)) (*Node, error) {
	var (
		insert    *sqlite3.Stmt
		tableName = fmt.Sprintf("snapshot_%s_%d", prefix, version)
		ordinal   int
		batchSize = 200_000
		log       = logz.Logger.With().Str("prefix", prefix).Logger()
		step      func() (*Node, error)
		lastWrite = time.Now()
	)

	err := conn.Exec(fmt.Sprintf("CREATE TABLE %s (ordinal int, version int, sequence int, bytes blob);", tableName))
	if err != nil {
		return nil, err
	}
	prepare := func() error {
		if err = conn.Begin(); err != nil {
			return err
		}
		insert, err = conn.Prepare(
			fmt.Sprintf("INSERT INTO %s (ordinal, version, sequence, bytes) VALUES (?, ?, ?, ?);", tableName))
		if err != nil {
			return err
		}
		return nil
	}
	flush := func() error {
		log.Info().Msgf("flush total=%s size=%s dur=%s wr/s=%s",
			humanize.Comma(int64(ordinal)),
			humanize.Comma(int64(batchSize)),
			time.Since(lastWrite).Round(time.Millisecond),
			humanize.Comma(int64(float64(batchSize)/time.Since(lastWrite).Seconds())),
		)
		err = errors.Join(conn.Commit(), insert.Close())
		lastWrite = time.Now()
		return err
	}
	maybeFlush := func() error {
		if ordinal%batchSize == 0 {
			if err = flush(); err != nil {
				return err
			}
			if err = prepare(); err != nil {
				return err
			}
		}
		return nil
	}
	if err = prepare(); err != nil {
		return nil, err
	}
	step = func() (*Node, error) {
		snapshotNode, err := nextFn()
		if err != nil {
			return nil, err
		}
		ordinal++

		node := &Node{
			key:           snapshotNode.Key,
			subtreeHeight: snapshotNode.Height,
			nodeKey:       NewNodeKey(snapshotNode.Version, uint32(ordinal)),
		}
		if node.subtreeHeight == 0 {
			node.value = snapshotNode.Value
			node.size = 1
			node._hash()
			nodeBz, err := node.Bytes()
			if err != nil {
				return nil, err
			}
			if err = insert.Exec(ordinal, snapshotNode.Version, ordinal, nodeBz); err != nil {
				return nil, err
			}
			if err = maybeFlush(); err != nil {
				return nil, err
			}
			return node, nil
		}

		node.leftNode, err = step()
		if err != nil {
			return nil, err
		}
		node.leftNodeKey = node.leftNode.nodeKey
		node.rightNode, err = step()
		if err != nil {
			return nil, err
		}
		node.rightNodeKey = node.rightNode.nodeKey

		node.size = node.leftNode.size + node.rightNode.size
		node._hash()
		node.leftNode = nil
		node.rightNode = nil

		nodeBz, err := node.Bytes()
		if err != nil {
			return nil, err
		}
		if err = insert.Exec(ordinal, snapshotNode.Version, ordinal, nodeBz); err != nil {
			return nil, err
		}
		if err = maybeFlush(); err != nil {
			return nil, err
		}
		return node, nil
	}
	root, err := step()
	if err != nil {
		return nil, err
	}
	if err = flush(); err != nil {
		return nil, err
	}
	if err = conn.Exec(fmt.Sprintf("CREATE INDEX %s_idx ON %s (ordinal);", tableName, tableName)); err != nil {
		return nil, err
	}
	if err = conn.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

func (sql *SqliteDb) WriteSnapshot(
	ctx context.Context, version int64, nextFn func() (*SnapshotNode, error), opts SnapshotOptions,
) (*Node, error) {
	snap := &sqliteSnapshot{
		ctx:       ctx,
		sql:       sql,
		batchSize: 400_000,
		version:   version,
		lastWrite: time.Now(),
		log:       sql.logger,
		writeTree: true,
	}
	if opts.WriteCheckpoint {
		if _, err := sql.nextShard(version); err != nil {
			return nil, err
		}
	}
	err := snap.sql.leafWrite.Exec(
		fmt.Sprintf(`CREATE TABLE snapshot_%d (ordinal int, version int, sequence int, bytes blob);`, version))
	if err != nil {
		return nil, err
	}
	if err = snap.prepareWrite(); err != nil {
		return nil, err
	}

	var (
		root           *Node
		uniqueVersions map[int64]struct{}
	)
	if opts.TraverseOrder == PostOrder {
		root, uniqueVersions, err = snap.restorePostOrderStep(nextFn, opts.StoreLeafValues)
	} else if opts.TraverseOrder == PreOrder {
		root, uniqueVersions, err = snap.restorePreOrderStep(nextFn, opts.StoreLeafValues)
	}
	if err != nil {
		return nil, err
	}

	if err = snap.flush(); err != nil {
		return nil, err
	}

	var versions []int64 // where is this used?
	for v := range uniqueVersions {
		versions = append(versions, v)
	}

	if err = sql.SaveRoot(version, root, true); err != nil {
		return nil, err
	}

	sql.logger.Info("creating table indexes")
	err = sql.leafWrite.Exec(fmt.Sprintf("CREATE INDEX snapshot_%d_idx ON snapshot_%d (ordinal);", version, version))
	if err != nil {
		return nil, err
	}
	err = snap.sql.treeWrite.Exec(fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", snap.version, snap.version))
	if err != nil {
		return nil, err
	}
	err = snap.sql.leafWrite.Exec("CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence)")
	if err != nil {
		return nil, err
	}

	return root, nil
}

type SnapshotNode struct {
	Key     []byte
	Value   []byte
	Version int64
	Height  int8
}

func (sql *SqliteDb) ImportSnapshotFromTable(version int64, traverseOrder TraverseOrderType, loadLeaves bool) (*Node, error) {
	read, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}

	var q *sqlite3.Stmt
	if traverseOrder == PostOrder {
		q, err = read.Prepare(fmt.Sprintf("SELECT version, sequence, bytes FROM snapshot_%d ORDER BY ordinal DESC", version))
	} else if traverseOrder == PreOrder {
		q, err = read.Prepare(fmt.Sprintf("SELECT version, sequence, bytes FROM snapshot_%d ORDER BY ordinal ASC", version))
	}
	if err != nil {
		return nil, err
	}
	defer func(q *sqlite3.Stmt) {
		err = q.Close()
		if err != nil {
			sql.logger.Error("error closing import query", "error", err)
		}
	}(q)

	imp := &sqliteImport{
		query:      q,
		pool:       sql.pool,
		loadLeaves: loadLeaves,
		since:      time.Now(),
		log:        sql.logger,
	}
	var root *Node
	if traverseOrder == PostOrder {
		root, err = imp.queryStepPostOrder()
	} else if traverseOrder == PreOrder {
		root, err = imp.queryStepPreOrder()
	}
	if err != nil {
		return nil, err
	}

	if !loadLeaves {
		return root, nil
	}

	h := root.hash
	rehashTree(root)
	if !bytes.Equal(h, root.hash) {
		return nil, fmt.Errorf("rehash failed; expected=%x, got=%x", h, root.hash)
	}

	return root, nil
}

func (sql *SqliteDb) ImportMostRecentSnapshot(targetVersion int64, traverseOrder TraverseOrderType, loadLeaves bool) (*Node, int64, error) {
	read, err := sql.getReadConn()
	if err != nil {
		return nil, 0, err
	}
	q, err := read.Prepare("SELECT tbl_name FROM changelog.sqlite_master WHERE type='table' AND name LIKE 'snapshot_%' ORDER BY name DESC")
	defer func(q *sqlite3.Stmt) {
		err = q.Close()
		if err != nil {
			sql.logger.Error("error closing import query", "error", err)
		}
	}(q)
	if err != nil {
		return nil, 0, err
	}

	var (
		name    string
		version int64
	)
	for {
		ok, err := q.Step()
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			return nil, 0, fmt.Errorf("no prior snapshot found version=%d path=%s", targetVersion, sql.opts.Path)
		}
		err = q.Scan(&name)
		if err != nil {
			return nil, 0, err
		}
		vs := name[len("snapshot_"):]
		if vs == "" {
			return nil, 0, fmt.Errorf("unexpected snapshot table name %s", name)
		}
		version, err = strconv.ParseInt(vs, 10, 64)
		if err != nil {
			return nil, 0, err
		}
		if version <= targetVersion {
			break
		}
	}

	root, err := sql.ImportSnapshotFromTable(version, traverseOrder, loadLeaves)
	if err != nil {
		return nil, 0, err
	}
	return root, version, err
}

func FindDbsInPath(path string) ([]string, error) {
	var paths []string
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Base(path) == "changelog.sqlite" {
			paths = append(paths, filepath.Dir(path))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

// TODO
// merge these two functions

func (snap *sqliteSnapshot) writeStep(node *Node) error {
	snap.ordinal++
	// Pre-order, NLR traversal
	// Visit this node
	nodeBz, err := node.Bytes()
	if err != nil {
		return err
	}
	err = snap.snapshotInsert.Exec(snap.ordinal, node.nodeKey.Version(), int(node.nodeKey.Sequence()), nodeBz)
	if err != nil {
		return err
	}

	if snap.ordinal%snap.batchSize == 0 {
		if err = snap.flush(); err != nil {
			return err
		}
		if err = snap.prepareWrite(); err != nil {
			return err
		}
	}

	if node.isLeaf() {
		return nil
	}

	// traverse left
	err = snap.writeStep(snap.getLeft(node))
	if err != nil {
		return err
	}

	// traverse right
	return snap.writeStep(snap.getRight(node))
}

func (snap *sqliteSnapshot) flush() error {
	select {
	case <-snap.ctx.Done():
		snap.log.Info(fmt.Sprintf("snapshot cancelled at ordinal=%s", humanize.Comma(int64(snap.ordinal))))
		errs := errors.Join(
			snap.snapshotInsert.Reset(),
			snap.snapshotInsert.Close(),
		)
		if errs != nil {
			return errs
		}
		if snap.writeTree {
			errs = errors.Join(
				snap.leafInsert.Reset(),
				snap.leafInsert.Close(),
				snap.treeInsert.Reset(),
				snap.treeInsert.Close(),
			)
		}
		errs = errors.Join(
			errs,
			snap.sql.leafWrite.Rollback(),
			snap.sql.leafWrite.Close(),
		)
		if errs != nil {
			return errs
		}
		if snap.writeTree {
			errs = errors.Join(
				errs,
				snap.sql.treeWrite.Rollback(),
				snap.sql.treeWrite.Close(),
			)
		}

		return errs
	default:
	}

	snap.log.Info(fmt.Sprintf("flush total=%s size=%s dur=%s wr/s=%s",
		humanize.Comma(int64(snap.ordinal)),
		humanize.Comma(int64(snap.batchSize)),
		time.Since(snap.lastWrite).Round(time.Millisecond),
		humanize.Comma(int64(float64(snap.batchSize)/time.Since(snap.lastWrite).Seconds())),
	))

	err := errors.Join(
		snap.sql.leafWrite.Commit(),
		snap.snapshotInsert.Close(),
	)
	if err != nil {
		return err
	}
	if snap.writeTree {
		err = errors.Join(
			snap.leafInsert.Close(),
			snap.sql.treeWrite.Commit(),
			snap.treeInsert.Close(),
		)
	}
	snap.lastWrite = time.Now()
	return err
}

func (snap *sqliteSnapshot) prepareWrite() error {
	err := snap.sql.leafWrite.Begin()
	if err != nil {
		return err
	}

	snap.snapshotInsert, err = snap.sql.leafWrite.Prepare(
		fmt.Sprintf("INSERT INTO snapshot_%d (ordinal, version, sequence, bytes) VALUES (?, ?, ?, ?);",
			snap.version))

	if snap.writeTree {
		err = snap.sql.treeWrite.Begin()
		if err != nil {
			return err
		}

		snap.leafInsert, err = snap.sql.leafWrite.Prepare("INSERT INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)")
		if err != nil {
			return err
		}
		snap.treeInsert, err = snap.sql.treeWrite.Prepare(
			fmt.Sprintf("INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", snap.version))
	}

	return err
}

func (snap *sqliteSnapshot) restorePostOrderStep(nextFn func() (*SnapshotNode, error), isStoreLeafValues bool) (*Node, map[int64]struct{}, error) {
	var (
		snapshotNode   *SnapshotNode
		err            error
		count          int
		stack          []*Node
		uniqueVersions = make(map[int64]struct{})
	)

	for {
		snapshotNode, err = nextFn()
		if err != nil || snapshotNode == nil {
			break
		}

		ordinal := snap.ordinal

		uniqueVersions[snapshotNode.Version] = struct{}{}
		node := &Node{
			key:           snapshotNode.Key,
			subtreeHeight: snapshotNode.Height,
			nodeKey:       NewNodeKey(snapshotNode.Version, uint32(ordinal)),
		}

		stackSize := len(stack)
		if node.isLeaf() {
			node.value = snapshotNode.Value
			node.size = 1
			node._hash()
			if !isStoreLeafValues {
				node.value = nil
			}

			count++
			if err := snap.writeSnapNode(node, snapshotNode.Version, count, ordinal, count); err != nil {
				return nil, nil, err
			}
		} else if stackSize >= 2 && stack[stackSize-1].subtreeHeight < node.subtreeHeight && stack[stackSize-2].subtreeHeight < node.subtreeHeight {
			node.leftNode = stack[stackSize-2]
			node.leftNodeKey = node.leftNode.nodeKey
			node.rightNode = stack[stackSize-1]
			node.rightNodeKey = node.rightNode.nodeKey
			node.size = node.leftNode.size + node.rightNode.size
			node._hash()
			stack = stack[:stackSize-2]

			node.leftNode = nil
			node.rightNode = nil

			count++
			if err := snap.writeSnapNode(node, snapshotNode.Version, count, ordinal, count); err != nil {
				return nil, nil, err
			}
		}

		stack = append(stack, node)
		snap.ordinal++
	}

	if err != nil && !errors.Is(err, ErrorExportDone) {
		return nil, nil, err
	}

	if len(stack) != 1 {
		return nil, nil, fmt.Errorf("expected stack size 1, got %d", len(stack))
	}

	return stack[0], uniqueVersions, nil
}

func (snap *sqliteSnapshot) restorePreOrderStep(nextFn func() (*SnapshotNode, error), isStoreLeafValues bool) (*Node, map[int64]struct{}, error) {
	var (
		count          int
		step           func() (*Node, error)
		uniqueVersions = make(map[int64]struct{})
	)

	step = func() (*Node, error) {
		snapshotNode, err := nextFn()
		if err != nil {
			return nil, err
		}

		ordinal := snap.ordinal
		snap.ordinal++

		node := &Node{
			key:           snapshotNode.Key,
			subtreeHeight: snapshotNode.Height,
			nodeKey:       NewNodeKey(snapshotNode.Version, uint32(ordinal)),
		}

		if node.isLeaf() {
			node.value = snapshotNode.Value
			node.size = 1
			node._hash()
			if !isStoreLeafValues {
				node.value = nil
			}
		} else {
			node.leftNode, err = step()
			if err != nil {
				return nil, err
			}
			node.leftNodeKey = node.leftNode.nodeKey
			node.rightNode, err = step()
			if err != nil {
				return nil, err
			}
			node.rightNodeKey = node.rightNode.nodeKey

			node.size = node.leftNode.size + node.rightNode.size
			node._hash()
			node.leftNode = nil
			node.rightNode = nil
			uniqueVersions[snapshotNode.Version] = struct{}{}
		}

		count++
		if err := snap.writeSnapNode(node, snapshotNode.Version, ordinal, ordinal, count); err != nil {
			return nil, err
		}
		snap.ordinal++

		return node, nil
	}

	node, err := step()

	return node, uniqueVersions, err
}

func (snap *sqliteSnapshot) writeSnapNode(node *Node, version int64, ordinal, sequence, count int) error {
	nodeBz, err := node.Bytes()
	if err != nil {
		return err
	}
	if err = snap.snapshotInsert.Exec(ordinal, version, sequence, nodeBz); err != nil {
		return err
	}
	if snap.writeTree {
		if node.isLeaf() {
			if err = snap.leafInsert.Exec(version, ordinal, nodeBz); err != nil {
				return err
			}
		} else {
			if err = snap.treeInsert.Exec(version, sequence, nodeBz); err != nil {
				return err
			}
		}
	}

	if count%snap.batchSize == 0 {
		if err := snap.flush(); err != nil {
			return err
		}
		if err := snap.prepareWrite(); err != nil {
			return err
		}
	}

	return nil
}

func rehashTree(node *Node) {
	if node.isLeaf() {
		return
	}
	node.hash = nil

	rehashTree(node.leftNode)
	rehashTree(node.rightNode)

	node._hash()
}

type sqliteImport struct {
	query      *sqlite3.Stmt
	pool       *NodePool
	loadLeaves bool

	i     int64
	since time.Time
	log   Logger
}

func (sqlImport *sqliteImport) queryStepPreOrder() (node *Node, err error) {
	sqlImport.i++
	if sqlImport.i%1_000_000 == 0 {
		sqlImport.log.Debug(fmt.Sprintf("import: nodes=%s, node/s=%s",
			humanize.Comma(sqlImport.i),
			humanize.Comma(int64(float64(1_000_000)/time.Since(sqlImport.since).Seconds())),
		))
		sqlImport.since = time.Now()
	}

	hasRow, err := sqlImport.query.Step()
	if !hasRow {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var bz sqlite3.RawBytes
	var version, seq int
	err = sqlImport.query.Scan(&version, &seq, &bz)
	if err != nil {
		return nil, err
	}
	nodeKey := NewNodeKey(int64(version), uint32(seq))
	node, err = MakeNode(sqlImport.pool, nodeKey, bz)
	if err != nil {
		return nil, err
	}

	if node.isLeaf() && sqlImport.i > 1 {
		if sqlImport.loadLeaves {
			return node, nil
		}
		sqlImport.pool.Put(node)
		return nil, nil
	}

	node.leftNode, err = sqlImport.queryStepPreOrder()
	if err != nil {
		return nil, err
	}
	node.rightNode, err = sqlImport.queryStepPreOrder()
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (sqlImport *sqliteImport) queryStepPostOrder() (node *Node, err error) {
	sqlImport.i++
	if sqlImport.i%1_000_000 == 0 {
		sqlImport.log.Debug(fmt.Sprintf("import: nodes=%s, node/s=%s",
			humanize.Comma(sqlImport.i),
			humanize.Comma(int64(float64(1_000_000)/time.Since(sqlImport.since).Seconds())),
		))
		sqlImport.since = time.Now()
	}

	hasRow, err := sqlImport.query.Step()
	if !hasRow {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var bz sqlite3.RawBytes
	var version, seq int
	err = sqlImport.query.Scan(&version, &seq, &bz)
	if err != nil {
		return nil, err
	}
	nodeKey := NewNodeKey(int64(version), uint32(seq))
	node, err = MakeNode(sqlImport.pool, nodeKey, bz)
	if err != nil {
		return nil, err
	}

	if node.isLeaf() && sqlImport.i > 1 {
		if sqlImport.loadLeaves {
			return node, nil
		}
		sqlImport.pool.Put(node)
		return nil, nil
	}

	node.rightNode, err = sqlImport.queryStepPostOrder()
	if err != nil {
		return nil, err
	}
	node.leftNode, err = sqlImport.queryStepPostOrder()
	if err != nil {
		return nil, err
	}

	return node, nil
}
