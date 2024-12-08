package iavl

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

// SqliteKVStore is a generic KV store which uses sqlite as the backend and be used by applications to store and
// retrieve generic key-value pairs, probably for metadata.
type SqliteKVStore struct {
	options SqliteDbOptions
	write   *sqlite3.Conn
	read    *sqlite3.Conn
	lock    *sync.Mutex
}

func NewSqliteKVStore(opts SqliteDbOptions) (kv *SqliteKVStore, err error) {
	if opts.Path == "" {
		return nil, errors.New("path cannot be empty")
	}
	if opts.WalSize == 0 {
		opts.WalSize = 50 * 1024 * 1024
	}

	pageSize := os.Getpagesize()
	kv = &SqliteKVStore{options: opts, lock: &sync.Mutex{}}
	kv.write, err = sqlite3.Open(fmt.Sprintf(
		"file:%s?_journal_mode=WAL&_synchronous=OFF&&_wal_autocheckpoint=%d", opts.Path, pageSize/opts.WalSize))
	if err != nil {
		return nil, err
	}

	// Create the tables if they don't exist
	if err = kv.write.Exec("CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB)"); err != nil {
		return nil, err
	}

	kv.read, err = sqlite3.Open(fmt.Sprintf("file:%s?mode=ro", opts.Path))
	if err != nil {
		return nil, err
	}

	return kv, nil
}

func (kv *SqliteKVStore) Set(key []byte, value []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	if err := kv.write.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value); err != nil {
		return err
	}
	return nil
}

func (kv *SqliteKVStore) Get(key []byte) (value []byte, err error) {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	stmt, err := kv.read.Prepare("SELECT value FROM kv WHERE key = ?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if err = stmt.Bind(key); err != nil {
		return nil, err
	}
	ok, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	if err = stmt.Scan(&value); err != nil {
		return nil, err
	}

	return value, nil
}

func (kv *SqliteKVStore) Delete(key []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	if err := kv.write.Exec("DELETE FROM kv WHERE key = ?", key); err != nil {
		return err
	}
	return nil
}
