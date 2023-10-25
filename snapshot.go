package iavl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/dustin/go-humanize"
)

func (sql *SqliteDb) Snapshot(ctx context.Context, tree *Tree, version int64) error {
	err := sql.leafWrite.Exec(
		fmt.Sprintf("CREATE TABLE snapshot_%d (ordinal int, version int, sequence int, bytes blob);", version))
	if err != nil {
		return err
	}
	err = tree.LoadVersion(version)
	if err != nil {
		return err
	}

	snapshot := &sqliteSnapshot{
		ctx:       ctx,
		conn:      sql.leafWrite,
		batchSize: 200_000,
		version:   version,
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
	log.Info().Str("path", sql.opts.Path).Msgf("creating index on snapshot_%d", version)
	err = sql.leafWrite.Exec(fmt.Sprintf("CREATE INDEX snapshot_%d_idx ON snapshot_%d (ordinal);", version, version))
	return err
}

func (sql *SqliteDb) ImportSnapshot(version int64, loadLeaves bool) (*Node, error) {
	read, err := sql.getReadConn()
	if err != nil {
		return nil, err
	}
	q, err := read.Prepare(fmt.Sprintf("SELECT version, sequence, bytes FROM snapshot_%d ORDER BY ordinal", version))
	if err != nil {
		return nil, err
	}
	defer func(q *sqlite3.Stmt) {
		err = q.Close()
		if err != nil {
			log.Error().Err(err).Msg("error closing import query")
		}
	}(q)

	imp := &sqliteImport{
		query:      q,
		pool:       sql.pool,
		loadLeaves: loadLeaves,
		since:      time.Now(),
	}
	root, err := imp.step()
	if err != nil {
		return nil, err
	}

	if !loadLeaves {
		return root, nil
	}

	// if full tree was loaded then rehash the full tree to validate integrity of the snapshot
	tree := NewTree(sql, sql.pool)
	if err = tree.LoadVersion(version); err != nil {
		return nil, err
	}

	// TODO ensure node.NodeKey.Version() is correct
	rehashTree(root)
	if !bytes.Equal(root.hash, tree.root.hash) {
		return nil, fmt.Errorf("root hash mismatch: %x != %x", root.hash, tree.root.hash)
	}

	return root, nil
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

type sqliteSnapshot struct {
	ctx       context.Context
	conn      *sqlite3.Conn
	batch     *sqlite3.Stmt
	lastWrite time.Time
	ordinal   int
	batchSize int
	version   int64
	getLeft   func(*Node) *Node
	getRight  func(*Node) *Node
}

func (snap *sqliteSnapshot) writeStep(node *Node) error {
	snap.ordinal++
	// Pre-order, NLR traversal
	// Visit this node
	nodeBz, err := node.Bytes()
	if err != nil {
		return err
	}
	err = snap.batch.Exec(snap.ordinal, node.nodeKey.Version(), int(node.nodeKey.Sequence()), nodeBz)
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
		log.Info().Msgf("snapshot cancelled at ordinal=%s", humanize.Comma(int64(snap.ordinal)))
		return errors.Join(
			snap.batch.Reset(),
			snap.batch.Close(),
			snap.conn.Rollback(),
			snap.conn.Close())
	default:
	}

	log.Info().Msgf("flush total=%s, batch=%s, dur=%s, wr/s=%s",
		humanize.Comma(int64(snap.ordinal)),
		humanize.Comma(int64(snap.batchSize)),
		time.Since(snap.lastWrite).Round(time.Millisecond),
		humanize.Comma(int64(float64(snap.batchSize)/time.Since(snap.lastWrite).Seconds())),
	)

	err := snap.conn.Commit()
	if err != nil {
		return err
	}
	err = snap.batch.Close()
	if err != nil {
		return err
	}
	snap.lastWrite = time.Now()
	return nil
}

func (snap *sqliteSnapshot) prepareWrite() error {
	err := snap.conn.Begin()
	if err != nil {
		return err
	}
	insert := fmt.Sprintf("INSERT INTO snapshot_%d (ordinal, version, sequence, bytes) VALUES (?, ?, ?, ?);",
		snap.version)
	snap.batch, err = snap.conn.Prepare(insert)
	return err
}

func rehashTree(node *Node) {
	if node.isLeaf() {
		return
	}
	node.hash = nil

	rehashTree(node.leftNode)
	rehashTree(node.rightNode)

	node._hash(node.nodeKey.Version())
}

type sqliteImport struct {
	query      *sqlite3.Stmt
	pool       *NodePool
	loadLeaves bool

	i     int64
	since time.Time
}

func (sqlImport *sqliteImport) step() (node *Node, err error) {
	sqlImport.i++
	if sqlImport.i%1_000_000 == 0 {
		log.Debug().Msgf("import: nodes=%s, node/s=%s",
			humanize.Comma(sqlImport.i),
			humanize.Comma(int64(float64(1_000_000)/time.Since(sqlImport.since).Seconds())),
		)
		sqlImport.since = time.Now()
	}

	hasRow, err := sqlImport.query.Step()
	if !hasRow {
		return nil, sqlImport.query.Reset()
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

	if node.isLeaf() {
		if sqlImport.loadLeaves {
			return node, nil
		}
		sqlImport.pool.Put(node)
		return nil, nil
	}

	node.leftNode, err = sqlImport.step()
	if err != nil {
		return nil, err
	}
	node.rightNode, err = sqlImport.step()
	if err != nil {
		return nil, err
	}
	return node, nil
}
