package iavl

import (
	"sync"
	"testing"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/cosmos/iavl/v2/testutil"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestBuildSqlite(t *testing.T) {
	dir := t.TempDir()
	t.Logf("dir: %s", dir)

	sql, err := NewSqliteDb(NewNodePool(), SqliteDbOptions{Path: dir})

	require.NoError(t, err)

	gen := testutil.OsmoLike()
	version1 := gen.Iterator.Nodes()
	var count int
	require.Equal(t, int64(1), gen.Iterator.Version())

	since := time.Now()

	require.NoError(t, err)
	_, err = sql.nextShard(1)
	require.NoError(t, err)
	conn := sql.treeWrite

	err = conn.Exec("CREATE TABLE node (seq INTEGER, version INTEGER, hash BLOB, key BLOB, height INTEGER, size INTEGER, l_seq INTEGER, l_version INTEGER, r_seq INTEGER, r_version INTEGER)")
	require.NoError(t, err)

	// enable this to demonstrate the slowness of a blob key index
	// err = conn.Exec("CREATE INDEX node_key_idx ON node (key)")
	err = conn.Exec("CREATE INDEX node_key_idx ON node (version, seq)")
	require.NoError(t, err)

	err = conn.Exec("CREATE INDEX tree_idx ON tree_1 (version, sequence)")
	require.NoError(t, err)

	require.NoError(t, conn.Begin())

	var stmt *sqlite3.Stmt
	stmt, err = conn.Prepare("INSERT INTO node(version, seq, hash, key, height, size, l_seq, l_version, r_seq, r_version)" +
		"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

	require.NoError(t, err)

	startTime := time.Now()
	batchSize := 200_000
	// set to -1 to run the full set of 40M nodes
	limit := 600_000
	// nodeBz := new(bytes.Buffer)
	for ; version1.Valid(); err = version1.Next() {
		require.NoError(t, err)
		if count >= limit {
			break
		}

		node := version1.GetNode()
		lnk := NewNodeKey(1, uint32(count+1))
		rnk := NewNodeKey(1, uint32(count+2))
		n := &Node{
			key: node.Key, hash: node.Key[:32],
			subtreeHeight: 13, size: 4, leftNodeKey: lnk, rightNodeKey: rnk,
		}

		// nodeBz.Reset()
		// require.NoError(t, n.WriteBytes(nodeBz))

		// tree table
		// nk := NewNodeKey(1, uint32(count))
		// nodeBz, err := n.Bytes()
		// require.NoError(t, err)
		// err = stmt.Exec(int(nk.Version()), int(nk.Sequence()), nodeBz)
		// require.NoError(t, err)

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
			// stmt, err = newBatch()
			// require.NoError(t, err)
			require.NoError(t, conn.Begin())
			t.Logf("nodes=%s dur=%s; rate=%s",
				humanize.Comma(int64(count)),
				time.Since(since).Round(time.Millisecond),
				humanize.Comma(int64(float64(batchSize)/time.Since(since).Seconds())))
			since = time.Now()
		}
		count++
		require.NoError(t, err)
	}

	t.Log("final commit")
	require.NoError(t, conn.Commit())
	t.Logf("total dur=%s rate=%s",
		time.Since(startTime).Round(time.Millisecond),
		humanize.Comma(int64(40_000_000/time.Since(startTime).Seconds())),
	)
	require.NoError(t, stmt.Close())
	require.NoError(t, sql.Close())
}

func TestNodeKeyFormat(t *testing.T) {
	nk := NewNodeKey(100, 2)
	require.NotNil(t, nk)
	k := (int(nk.Version()) << 32) | int(nk.Sequence())
	t.Logf("k: %d - %x\n", k, k)
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
	t.Logf("res: %s\n", res)
}

func Test_NewSqliteDb(t *testing.T) {
	dir := t.TempDir()
	sql, err := NewSqliteDb(NewNodePool(), SqliteDbOptions{Path: dir})
	require.NoError(t, err)
	require.NotNil(t, sql)
}

func Test_ConcurrentDetach(t *testing.T) {
	t.Skipf("this test will sometimes panic within a panic, kept to show what doesn't work")

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
	t.Skipf("this test will panic within a panic, kept to show what doesn't work")
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
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					errs <- r.(error)
				}
			}()
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

func Test_ConcurrentIndexRead(t *testing.T) {
	dir := t.TempDir()
	seed := uint64(1234)
	count := 1_000_000
	conn, err := sqlite3.Open(dir + "/db.db")
	require.NoError(t, err)
	r := rand.New(rand.NewSource(seed))
	require.NoError(t, conn.Exec("PRAGMA synchronous = OFF"))
	require.NoError(t, conn.Exec("PRAGMA journal_mode = WAL"))
	err = conn.Exec("CREATE TABLE foo (id BLOB, val INTEGER)")
	require.NoError(t, err)
	err = conn.Exec("CREATE TABLE bar (id BLOB, val INTEGER)")
	require.NoError(t, err)
	bz := make([]byte, 32)
	t.Log("insert 100_000 rows")
	require.NoError(t, conn.Begin())
	for i := 0; i < count; i++ {
		_, err = r.Read(bz)
		require.NoError(t, err)
		err = conn.Exec("INSERT INTO foo VALUES (?, ?)", bz, i)
		require.NoError(t, err)
		err = conn.Exec("INSERT INTO bar VALUES (?, ?)", bz, i)
		require.NoError(t, err)
		if i%100_000 == 0 {
			require.NoError(t, conn.Commit())
			require.NoError(t, conn.Begin())
			t.Log("inserted", i)
		}
	}
	require.NoError(t, conn.Commit())
	r = rand.New(rand.NewSource(seed))
	reader, err := sqlite3.Open(dir + "/db.db")
	require.NoError(t, err)
	t.Log("query 100_000 rows")
	stmt, err := reader.Prepare("SELECT val FROM bar WHERE id = ?")
	require.NoError(t, err)
	var v int
	go func(q *sqlite3.Stmt) {
		for i := 0; i < count; i++ {
			n, err := r.Read(bz)
			require.NoError(t, err)
			require.Equal(t, 32, n)
			err = q.Bind(bz)
			require.NoError(t, err)
			hasRow, err := q.Step()
			require.NoError(t, err)
			require.True(t, hasRow)
			err = q.Scan(&v)
			require.NoError(t, err)
			require.Equal(t, i, v)
			require.NoError(t, q.Reset())
		}
	}(stmt)
	err = conn.Exec("CREATE INDEX foo_idx ON foo (id)")
	require.NoError(t, err)
}
