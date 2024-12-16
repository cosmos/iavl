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
	conns map[int64]*sqliteConnection
}

func newHotConnectionFactory() *hotConnectionFactory {
	return &hotConnectionFactory{conns: make(map[int64]*sqliteConnection)}
}

func (f *hotConnectionFactory) make(version int64) (*sqliteConnection, error) {
	if version == -1 {
		// temporary hack until pruning is re-implemented and shard life cycle is worked out
		version = 1
	}
	if conn, ok := f.conns[version]; ok {
		return conn, nil
	}
	return nil, fmt.Errorf("no connection for version %d", version)
}

func (f *hotConnectionFactory) close() error {
	var c *sqlite3.Conn
	for _, sqlConn := range f.conns {
		if c == nil {
			c = sqlConn.conn
		}
		if err := sqlConn.queryLeaf.Close(); err != nil {
			return err
		}
		if err := sqlConn.queryBranch.Close(); err != nil {
			return err
		}
	}
	if c != nil {
		return c.Close()
	}
	return nil
}
