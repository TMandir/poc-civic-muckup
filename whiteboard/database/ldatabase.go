// Package database provides persistent token storage backed by LevelDB.
package database

import (
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// frontendTokenKey identifies the single value holding the latest frontend token.
const frontendTokenKey = "Dummy-frontend-token-key"

// tokenCountKey identifies the total number of tokens submitted to the database.
const tokenCountKey = "dummy-token-count"

// Database is a concurrency-safe handle to the application's LevelDB store.
type Database struct {
	// mu prevents closing the store while an operation is in progress.
	mu sync.RWMutex
	// db is nil after the database has been closed.
	db *leveldb.DB
}

// Open opens or creates a LevelDB database at path.
func Open(path string) (*Database, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}

	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	return &Database{db: db}, nil
}

// StoreToken persists the latest frontend token, replacing any previously stored token.
// It returns nil only after LevelDB has synchronously flushed the write.
func (d *Database) StoreToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return errors.New("token is required")
	}
	if d == nil {
		return errors.New("database is not open")
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db == nil {
		return errors.New("database is closed")
	}

	count, err := d.tokenCountLocked()
	if err != nil {
		return err
	}
	if count == int(^uint(0)>>1) {
		return errors.New("token count is at its maximum value")
	}

	var batch leveldb.Batch
	batch.Put([]byte(frontendTokenKey), []byte(token))
	batch.Put([]byte(tokenCountKey), []byte(strconv.Itoa(count+1)))
	return d.db.Write(&batch, &opt.WriteOptions{Sync: true})
}

// Close releases the LevelDB resources; calling Close more than once is safe.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db == nil {
		return nil
	}

	err := d.db.Close()
	d.db = nil
	return err
}
