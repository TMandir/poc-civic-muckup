// Package database provides persistent local token storage for each peer.
package database

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Record is the replicated unit of data stored by each node.
type Record struct {
	FrontendTokenKey string    `json:"frontendTokenKey"`
	Token            string    `json:"token"`
	Information      string    `json:"information"`
	UpdatedAt        time.Time `json:"updatedAt"`
	Origin           string    `json:"origin"`
}

// Store owns one node's LevelDB database and protects close operations.
type Store struct {
	mu sync.RWMutex
	db *leveldb.DB
}

// Open creates or opens a LevelDB directory for this node.
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("open LevelDB: %w", err)
	}
	return &Store{db: db}, nil
}

// Lookup returns one record from this node's local database.
func (s *Store) Lookup(key string) (Record, error) {
	if strings.TrimSpace(key) == "" {
		return Record{}, errors.New("frontendTokenKey is required")
	}
	if s == nil {
		return Record{}, errors.New("database is not open")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return Record{}, errors.New("database is closed")
	}
	encoded, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return Record{}, err
	}
	var record Record
	if err := json.Unmarshal(encoded, &record); err != nil {
		return Record{}, fmt.Errorf("decode record: %w", err)
	}
	return record, nil
}

// ApplyIfNewer persists a valid record if it wins the deterministic
// last-write-wins ordering. It reports whether the local database changed.
func (s *Store) ApplyIfNewer(record Record) (bool, error) {
	if err := Validate(record); err != nil {
		return false, err
	}
	if s == nil {
		return false, errors.New("database is not open")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return false, errors.New("database is closed")
	}

	key := []byte(record.FrontendTokenKey)
	encoded, err := s.db.Get(key, nil)
	if err == nil {
		var current Record
		if err := json.Unmarshal(encoded, &current); err != nil {
			return false, fmt.Errorf("decode existing record: %w", err)
		}
		if CompareVersion(record, current) <= 0 {
			return false, nil
		}
	} else if !errors.Is(err, leveldb.ErrNotFound) {
		return false, fmt.Errorf("read existing record: %w", err)
	}

	encoded, err = json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("encode record: %w", err)
	}
	if err := s.db.Put(key, encoded, &opt.WriteOptions{Sync: true}); err != nil {
		return false, fmt.Errorf("persist record: %w", err)
	}
	return true, nil
}

// List returns all records in key order for full synchronization with a peer.
func (s *Store) List() ([]Record, error) {
	if s == nil {
		return nil, errors.New("database is not open")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return nil, errors.New("database is closed")
	}
	records := make([]Record, 0)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var record Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			iter.Release()
			return nil, fmt.Errorf("decode record: %w", err)
		}
		records = append(records, record)
	}
	if err := iter.Error(); err != nil {
		iter.Release()
		return nil, fmt.Errorf("iterate records: %w", err)
	}
	iter.Release()
	return records, nil
}

// Close releases the database. Calling it more than once is safe.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// Validate rejects empty or unversioned records received from forms or peers.
func Validate(record Record) error {
	switch {
	case strings.TrimSpace(record.FrontendTokenKey) == "":
		return errors.New("frontendTokenKey is required")
	case strings.TrimSpace(record.Token) == "":
		return errors.New("token is required")
	case strings.TrimSpace(record.Information) == "":
		return errors.New("token information is required")
	case record.UpdatedAt.IsZero():
		return errors.New("updatedAt is required")
	case strings.TrimSpace(record.Origin) == "":
		return errors.New("origin peer ID is required")
	default:
		return nil
	}
}

// CompareVersion defines one consistent ordering for concurrent updates.
func CompareVersion(first, second Record) int {
	if first.UpdatedAt.After(second.UpdatedAt) {
		return 1
	}
	if first.UpdatedAt.Before(second.UpdatedAt) {
		return -1
	}
	if first.Origin > second.Origin {
		return 1
	}
	if first.Origin < second.Origin {
		return -1
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	switch {
	case string(firstJSON) > string(secondJSON):
		return 1
	case string(firstJSON) < string(secondJSON):
		return -1
	default:
		return 0
	}
}
