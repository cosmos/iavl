package iavl

import (
	"fmt"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type connectionFactory interface {
	make(int64) (*sqliteConnection, error)
	close() error
}

type sqliteConnection struct {
	shardID     int64
	conn        *sqlite3.Conn
	queryLeaf   *sqlite3.Stmt
	queryBranch *sqlite3.Stmt
}

func (s *sqliteConnection) Close() error {
	if err := s.queryLeaf.Close(); err != nil {
		return err
	}
	if err := s.queryBranch.Close(); err != nil {
		return err
	}
	return s.conn.Close()
}

type readConnectionFactory struct {
	sql   *SqliteDb
	conns map[int64]*sqliteConnection
}

func newReadConnectionFactory(sql *SqliteDb) connectionFactory {
	return &readConnectionFactory{sql: sql, conns: make(map[int64]*sqliteConnection)}
}

func (f *readConnectionFactory) make(version int64) (*sqliteConnection, error) {
	// TODO: shard locking?
	// this code path is used from public Get APIs, therefore it probably needs locking on shards
	// to coordinate with pruning re-orgs.
	// OTOH, since pointer writes (*VersionRange) are atomic, and the shardID is immutable, it might be safe to
	// delay the deletion of a shard until all readers have finished with it; exponential back off on conncection close followed
	// by shard deletion might be sufficient.
	shardID := f.sql.shards.FindMemoized(version)
	if shardID == -1 {
		return nil, fmt.Errorf("shard not found version=%d shards=%v", version, f.sql.shards.versions)
	}
	if conn, ok := f.conns[shardID]; ok {
		return conn, nil
	}
	var err error
	f.conns[version], err = f.sql.newShardReadConnection(shardID)
	if err != nil {
		return nil, err
	}
	return f.conns[version], nil
}

func (f *readConnectionFactory) close() error {
	for _, conn := range f.conns {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

type hotConnectionFactory struct {
	main  *sqlite3.Conn
	opts  SqliteDbOptions
	conns map[int64]*sqliteConnection
}

func newHotConnectionFactory(hub *sqlite3.Conn, opts SqliteDbOptions) *hotConnectionFactory {
	conns := make(map[int64]*sqliteConnection)
	return &hotConnectionFactory{main: hub, opts: opts, conns: conns}
}

func (f *hotConnectionFactory) make(version int64) (*sqliteConnection, error) {
	if conn, ok := f.conns[version]; ok {
		return conn, nil
	}
	return nil, fmt.Errorf("no connection for version=%d path=%s", version, f.opts.shortPath())
}

func (f *hotConnectionFactory) addShard(shardID int64) error {
	s := &sqliteConnection{
		shardID: shardID,
		conn:    f.main,
	}
	if _, ok := f.conns[shardID]; ok {
		return fmt.Errorf("shard %d already connected", shardID)
	}

	err := s.conn.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS shard_%d", f.opts.treeConnectionString(shardID), shardID))
	if err != nil {
		return err
	}
	s.queryLeaf, err = s.conn.Prepare(
		fmt.Sprintf("SELECT bytes FROM shard_%d.leaf WHERE version = ? AND sequence = ?", shardID))
	if err != nil {
		return err
	}
	s.queryBranch, err = s.conn.Prepare(
		fmt.Sprintf("SELECT bytes FROM shard_%d.tree WHERE version = ? AND sequence = ?", shardID))
	if err != nil {
		return err
	}
	f.conns[shardID] = s
	return nil
}

func (f *hotConnectionFactory) removeShard(shardID int64) error {
	if conn, ok := f.conns[shardID]; ok {
		if err := conn.queryBranch.Close(); err != nil {
			return err
		}
		if err := conn.queryLeaf.Close(); err != nil {
			return err
		}
		if err := conn.conn.Exec(fmt.Sprintf("DETACH DATABASE shard_%d", shardID)); err != nil {
			return err
		}
		delete(f.conns, shardID)
		return nil
	}
	return fmt.Errorf("shard %d not connected", shardID)
}

func (f *hotConnectionFactory) close() error {
	for _, sqlConn := range f.conns {
		if err := sqlConn.queryLeaf.Close(); err != nil {
			return err
		}
		if err := sqlConn.queryBranch.Close(); err != nil {
			return err
		}
	}
	return f.main.Close()
}

func (f *hotConnectionFactory) clone() *hotConnectionFactory {
	conns := make(map[int64]*sqliteConnection)
	for k, v := range f.conns {
		conns[k] = v
	}
	return &hotConnectionFactory{main: f.main, opts: f.opts, conns: conns}
}
