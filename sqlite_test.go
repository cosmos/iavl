package iavl

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
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

*/

var testDbLocation = "/tmp/sqlite_test"

func TestBuildSqlite(t *testing.T) {
	dir := t.TempDir()
	t.Logf("using temp dir %s", dir)

	sql, err := NewSqliteDb(NewNodePool(), SqliteDbOptions{Path: dir})

	require.NoError(t, err)

	gen := testutil.OsmoLike()
	version1 := gen.Iterator.Nodes()
	var count int
	require.Equal(t, int64(1), gen.Iterator.Version())

	since := time.Now()

	conn, err := sql.rootConnection()
	require.NoError(t, err)

	err = conn.Exec("CREATE TABLE node (seq INTEGER, version INTEGER, hash BLOB, key BLOB, height INTEGER, size INTEGER, l_seq INTEGER, l_version INTEGER, r_seq INTEGER, r_version INTEGER)")
	require.NoError(t, err)

	err = conn.Exec("CREATE INDEX trie_idx ON node (key)")
	require.NoError(t, err)
	err = conn.Exec("CREATE INDEX tree_idx ON tree (version, sequence)")
	require.NoError(t, err)

	require.NoError(t, conn.Begin())

	var stmt *sqlite3.Stmt
	stmt, err = conn.Prepare("INSERT INTO node(version, seq, hash, key, height, size, l_seq, l_version, r_seq, r_version)" +
		"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

	require.NoError(t, err)

	startTime := time.Now()
	batchSize := 200_000
	//nodeBz := new(bytes.Buffer)
	for ; version1.Valid(); err = version1.Next() {
		node := version1.GetNode()
		lnk := NewNodeKey(1, uint32(count+1))
		rnk := NewNodeKey(1, uint32(count+2))
		n := &Node{key: node.Key, hash: node.Key[:32],
			subtreeHeight: 13, size: 4, leftNodeKey: lnk, rightNodeKey: rnk}

		//nodeBz.Reset()
		//require.NoError(t, n.WriteBytes(nodeBz))

		// tree table
		//nk := NewNodeKey(1, uint32(count))
		//nodeBz, err := n.Bytes()
		//require.NoError(t, err)
		//err = stmt.Exec(int(nk.Version()), int(nk.Sequence()), nodeBz)
		//require.NoError(t, err)

		// node table
		err = stmt.Exec(
			1,          // version
			count,      // seq
			n.key[:32], // hash
			n.key,      // key
			13,         // height
			4,          // size
			count+1,    // l_seq
			1,          // l_version
			count+2,    // r_seq
			1,          // r_version
		)

		if count%batchSize == 0 {
			err := conn.Commit()
			require.NoError(t, err)
			//stmt, err = newBatch()
			//require.NoError(t, err)
			require.NoError(t, conn.Begin())
			log.Info().Msgf("nodes=%s dur=%s; rate=%s",
				humanize.Comma(int64(count)),
				time.Since(since).Round(time.Millisecond),
				humanize.Comma(int64(float64(batchSize)/time.Since(since).Seconds())))
			since = time.Now()
		}
		count++
		require.NoError(t, err)
	}

	log.Info().Msg("final commit")
	require.NoError(t, conn.Commit())
	log.Info().Msgf("total dur=%s rate=%s",
		time.Since(startTime).Round(time.Millisecond),
		humanize.Comma(int64(40_000_000/time.Since(startTime).Seconds())),
	)
	require.NoError(t, stmt.Close())
	require.NoError(t, sql.Close())
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

func Test_ConcurrentDetach(t *testing.T) {
	dir := t.TempDir()
	conn, err := sqlite3.Open(dir + "/one.db")
	require.NoError(t, err)
	err = conn.Exec("CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	err = conn.Exec("INSERT INTO foo VALUES (4)")
	require.NoError(t, err)

	conn2, err := sqlite3.Open(dir + "/two.db")
	require.NoError(t, err)
	err = conn2.Exec("CREATE TABLE bar (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	err = conn2.Exec("INSERT INTO bar VALUES (7)")
	require.NoError(t, err)
	require.NoError(t, conn2.Close())

	require.NoError(t, conn.Exec("ATTACH DATABASE ? AS two", dir+"/two.db"))
	err = conn.Exec("SELECT * FROM bar")
	require.NoError(t, err)

	conn3, err := sqlite3.Open(dir + "/three.db")
	require.NoError(t, err)
	err = conn3.Exec("CREATE TABLE bar (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	err = conn3.Exec("INSERT INTO bar VALUES (8)")
	require.NoError(t, err)
	require.NoError(t, conn3.Close())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		q, err := conn.Prepare("SELECT * FROM bar")
		require.NoError(t, err)
		for i := 0; i < 500_000; i++ {
			hasRow, err := q.Step()
			require.NoError(t, err)
			require.True(t, hasRow)
			var v int
			err = q.Scan(&v)
			require.NoError(t, err)
			require.Equal(t, 7, v)
			require.NoError(t, q.Reset())
		}
	}()

	require.NoError(t, conn.Exec("ATTACH DATABASE ? AS three", dir+"/three.db"))
	require.Error(t, conn.Exec("DETACH DATABASE ?", dir+"/two.db"))

	wg.Wait()
}

func Test_ConcurrentQuery(t *testing.T) {
	dir := t.TempDir()
	conn, err := sqlite3.Open(dir + "/one.db")
	require.NoError(t, err)
	err = conn.Exec("CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = conn.Exec("INSERT INTO foo VALUES (?)", i)
		require.NoError(t, err)
	}

	times := 10
	errs := make(chan error, times)
	checkErr := func(err error) {
		if err != nil {
			errs <- err
		}
	}
	wg := sync.WaitGroup{}

	for i := 0; i < times; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q, err := conn.Prepare("SELECT * FROM foo WHERE id = ?")
			require.NoError(t, err)
			checkErr(q.Bind(i))
			hasRow, err := q.Step()
			checkErr(err)
			require.True(t, hasRow)
			var v int
			err = q.Scan(&v)
			checkErr(err)
			require.Equal(t, i, v)
			checkErr(q.Close())
		}(i)
	}

	wg.Wait()
	close(errs)

	for i := 0; i < times; i++ {
		err := <-errs
		if err == nil {
			require.Fail(t, "expected an error")
			return
		}
	}
}
