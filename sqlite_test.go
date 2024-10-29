package iavl

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl-bench/bench"
	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"
	"github.com/stretchr/testify/require"
)

/*
Benchmarks measured from these leafRead-leafWrite tests below:

# SQLite

## Writes

no index:
- structured batch insert (node table) - 507,700 nodes/sec
- unstructured batch insert (tree table) re-use same bytes.Buffer - 444,000 nodes/sec
- unstructured batch insert (tree table) alloc new bytes.Buffer - 473,800 nodes/sec
  - !! surprising, why? because GC is async? probably worse in long tail run?

indexed:
- structured batch insert (node table) - 441,000 nodes/sec
  - the difference between indexed and not is not significant.  the only way I can explain this is that the writes
    below are sequential proceeding version number.  this will always be the case with the current node key.
- unstructured batch insert (tree table) - 414,000 nodes/sec

writing into a trie based table (indexed on key) will likely be *much* slower since it requires an order insertion
and potentially re-balancing the BTree index.
^^^ True, initial test started at 200k and quickly declined to 75k

## Reads

- fully memory mapped unstructured (tree table) - ~160,000 nodes/sec
- fully memory mapped structured (node table) - ~172,000 nodes/sec
- fully memory mapped structured (node table) leafRead by key []byte - ~160,000 nodes/sec

# LevelDB
Writes: 245,000 nodes/sec
Reads: 30,000 nodes/sec !!!

# Testing variables
- payload size
- PRIMARY KEY vs ROWID (index b-tree vs table b-tree)
- B*Tree index (WITHOUT ROWID + PRIMARY KEY) vs b-tree table (WITH ROWID)
- structured vs unstructured (sqlite raw columns vs bytes marshal)
- clustered reads (optimized for page size) vs uniformly distributed reads

*/

var testDbLocation = "/tmp/sqlite_test"

type sqliteTestFixture struct {
	t *testing.T

	batchSize int
	startTime time.Time
	since     time.Time
	conn      *sqlite3.Conn
	gen       bench.ChangesetGenerator
}

func newSqliteTestFixture(t *testing.T) *sqliteTestFixture {
	return &sqliteTestFixture{t: t, batchSize: 200_000}
}

func (f *sqliteTestFixture) init(dir string) {
	t := f.t
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	conn, err := sqlite3.Open(fmt.Sprintf("file:%s/sqlite.db", dir))
	require.NoError(t, err)
	// opt for typical page size instead of call to os
	pageSize := os.Getpagesize()
	// pageSize := 4 * 1024
	log.Info().Msgf("setting page size to %s", humanize.Bytes(uint64(pageSize)))
	require.NoError(t, conn.Exec(fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize)))
	require.NoError(t, conn.Exec("PRAGMA journal_mode=WAL;"))
	require.NoError(t, conn.Exec("PRAGMA synchronous=0;"))
	require.NoError(t, conn.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d;", (200*1000*1000)/pageSize)))

	q, err := conn.Prepare("PRAGMA synchronous;")
	require.NoError(t, err)
	hasRow, err := q.Step()
	require.NoError(t, err)
	require.True(t, hasRow)
	sync, ok, err := q.ColumnText(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "0", sync)
	require.NoError(t, q.Close())

	f.conn = conn
}

func (f *sqliteTestFixture) maybeWrite(count int) {
	if count%f.batchSize == 0 {
		require.NoError(f.t, f.conn.Commit())
		require.NoError(f.t, f.conn.Begin())
		log.Info().Msgf("nodes=%s dur=%s; rate=%s",
			humanize.Comma(int64(count)),
			time.Since(f.since).Round(time.Millisecond),
			humanize.Comma(int64(float64(f.batchSize)/time.Since(f.since).Seconds())))
		f.since = time.Now()
	}
}

func (f *sqliteTestFixture) finishWrite(title string) {
	require.NoError(f.t, f.conn.Commit())
	f.t.Logf("_%s_\ndur=%s rate=%s\ngen=%+v",
		title,
		time.Since(f.startTime).Round(time.Millisecond),
		humanize.Comma(int64(float64(f.gen.InitialSize)/time.Since(f.startTime).Seconds())),
		f.gen,
	)
	require.NoError(f.t, f.conn.Close())
}

func (f *sqliteTestFixture) startBatch() {
	f.startTime = time.Now()
	f.since = time.Now()
	require.NoError(f.t, f.conn.Begin())
}

func (f *sqliteTestFixture) genNodes(itr api.NodeIterator) []*api.Node {
	f.t.Logf("generating %s nodes", humanize.Comma(int64(f.gen.InitialSize)))
	nodes := make([]*api.Node, f.gen.InitialSize)
	var i int
	var err error
	for ; itr.Valid(); err = itr.Next() {
		require.NoError(f.t, err)
		n := itr.GetNode()
		n.Value = nil
		nodes[i] = n
		i++
	}
	return nodes
}

func (f *sqliteTestFixture) writeStructuredTableBTree(pregenNodes bool) {
	t := f.t
	itr, err := f.gen.Iterator()
	require.NoError(t, err)
	require.Equal(t, int64(1), itr.Version())

	err = f.conn.Exec(`
CREATE TABLE node (
	version INTEGER, 
	seq INTEGER, 
	hash BLOB, 
	key BLOB, 
	value BLOB,
	height INTEGER, 
	size INTEGER, 
	l_seq INTEGER, l_version INTEGER, 
	r_seq INTEGER, r_version INTEGER
)`)
	require.NoError(t, err)

	stmt, err := f.conn.Prepare("INSERT INTO node(version, seq, hash, key, height, size, l_seq, l_version, r_seq, r_version)" +
		"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

	var count int
	write := func(node *api.Node) {
		// node table
		err = stmt.Exec(
			1,             // version
			count,         // seq
			node.Key[:32], // hash
			node.Key,      // key
			13,            // height
			4,             // size
			count+1,       // l_seq
			1,             // l_version
			count+2,       // r_seq
			1,             // r_version
		)
		require.NoError(t, err)

		f.maybeWrite(count)
		count++
	}

	f.startBatch()
	version1 := itr.Nodes()
	require.NoError(t, err)

	if pregenNodes {
		require.Equal(t, int64(1), itr.Version())
		nodes := f.genNodes(version1)
		for _, node := range nodes {
			write(node)
		}
	} else {
		for ; version1.Valid(); err = version1.Next() {
			require.NoError(t, err)
			write(version1.GetNode())
		}
	}

	require.NoError(t, stmt.Close())
	f.finishWrite("write structured table b-tree")
}

func (f *sqliteTestFixture) readStructuredBTree(dir string, chunkSize int, tableBTree bool) {
	t := f.t
	var err error
	conn, err := sqlite3.Open(fmt.Sprintf("file:%s/sqlite.db", dir))
	require.NoError(t, err)
	require.NoError(t, conn.Exec(fmt.Sprintf("PRAGMA mmap_size=%d", 4*1000*1000*1000)))
	require.NoError(t, conn.Exec("PRAGMA page_cache=10000;"))

	var stmt *sqlite3.Stmt
	require.NoError(t, err)
	if tableBTree {
		stmt, err = conn.Prepare("SELECT hash, key, height, size, l_seq, l_version, r_seq, r_version FROM node WHERE ROWID = ?")
	} else {
		stmt, err = conn.Prepare("SELECT hash, key, height, size, l_seq, l_version, r_seq, r_version FROM node WHERE seq = ? AND version = ?")
	}
	require.NoError(t, err)

	var hash, key []byte
	var height, size, lSeq, lVersion, rSeq, rVersion int

	since := time.Now()
	startTime := time.Now()
	for i := 0; i < f.gen.InitialSize; {
		j := rand.Intn(f.gen.InitialSize-chunkSize-1) + chunkSize + 1

		for m := 0; m < chunkSize; m++ {
			i++
			if tableBTree {
				require.NoError(t, stmt.Bind(j+m))
			} else {
				require.NoError(t, stmt.Bind(j+m, 1))
			}
			hasRow, err := stmt.Step()
			require.NoError(t, err)
			require.Truef(t, hasRow, "no row for %d", j+m)
			require.NoError(t, stmt.Scan(&hash, &key, &height, &size, &lSeq, &lVersion, &rSeq, &rVersion))
			require.NoError(t, stmt.Reset())
			if i%250_000 == 0 {
				log.Info().Msgf("nodes=%s dur=%s; rate=%s",
					humanize.Comma(int64(i)),
					time.Since(since),
					humanize.Comma(int64(float64(250_000)/time.Since(since).Seconds())))
				since = time.Now()
			}
		}
	}

	t.Logf("_structured read_\nuse-primary-key=%t chunk-size=%d dur=%s rate=%s\ngen=%+v",
		tableBTree, chunkSize,
		time.Since(startTime).Round(time.Millisecond),
		humanize.Comma(int64(float64(f.gen.InitialSize)/time.Since(startTime).Seconds())),
		f.gen,
	)

	require.NoError(t, stmt.Close())
	require.NoError(t, conn.Close())
}

func TestSqlite_ReadWriteUpdate_Performance(t *testing.T) {
	dir := testDbLocation
	t.Logf("using temp dir %s", dir)
	gen := bench.ChangesetGenerator{
		StoreKey:         "test",
		Seed:             1234,
		KeyMean:          1500,
		KeyStdDev:        3,
		ValueMean:        5,
		ValueStdDev:      3,
		InitialSize:      7_000_000,
		FinalSize:        40_100_000,
		Versions:         10,
		ChangePerVersion: 100,
		DeleteFraction:   0.25,
	}
	fix := newSqliteTestFixture(t)
	fix.gen = gen

	// fix.init(dir)
	// fix.writeStructuredTableBTree(true)
	fix.readStructuredBTree(dir, 1, true)

	// fix.init(dir)
	// fix.writeStructuredTableBTree()
	// fix.readStructuredBTree(dir, 16, true)
}

func TestNodeKeyFormat(t *testing.T) {
	nk := NewNodeKey(100, 2)
	k := (int(nk.Version()) << 32) | int(nk.Sequence())
	fmt.Printf("k: %d - %x\n", k, k)
}

func TestFetchNode(t *testing.T) {
	pool := NewNodePool()
	conn, err := sqlite3.Open("/tmp/iavl-v2.db")
	require.NoError(t, err)
	q := "SELECT bytes FROM tree_1 WHERE version = 1 and sequence = 6756148"
	stmt, err := conn.Prepare(q)
	require.NoError(t, err)
	hasRow, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, hasRow)
	nodeBz, err := stmt.ColumnBlob(0)
	require.NoError(t, err)
	nk := NewNodeKey(1, 6756148)
	node, err := MakeNode(pool, nk, nodeBz)
	require.NoError(t, err)
	fmt.Printf("node: %v\n", node)
}

func TestMmap(t *testing.T) {
	tmpDir := t.TempDir()
	conn, err := sqlite3.Open(tmpDir + "/test.db")
	require.NoError(t, err)
	stmt, err := conn.Prepare("PRAGMA mmap_size=1000000000000")
	require.NoError(t, err)
	ok, err := stmt.Step()
	require.NoError(t, err)
	require.True(t, ok)

	stmt, err = conn.Prepare("PRAGMA mmap_size")
	require.NoError(t, err)
	ok, err = stmt.Step()
	require.NoError(t, err)
	require.True(t, ok)
	res, ok, err := stmt.ColumnRawString(0)
	require.True(t, ok)
	require.NoError(t, err)
	fmt.Printf("res: %s\n", res)
}

func Test_NewSqliteDb(t *testing.T) {
	dir := t.TempDir()
	sql, err := NewSqliteDb(NewNodePool(), SqliteDbOptions{Path: dir})
	require.NoError(t, err)
	require.NotNil(t, sql)
}
