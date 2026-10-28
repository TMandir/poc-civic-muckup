package main

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

// userTokenRecord is the replicated unit of data. UpdatedAt and Origin make
// concurrent writes comparable on every peer using the same deterministic rule.
type userTokenRecord struct {
	FrontendTokenKey string    `json:"frontendTokenKey"`
	Token            string    `json:"token"`
	Information      string    `json:"information"`
	UpdatedAt        time.Time `json:"updatedAt"`
	Origin           string    `json:"origin"`
}

// userTokenStore wraps one node's LevelDB database and serializes access with
// Close so in-flight operations cannot race database shutdown.
type userTokenStore struct {
	mu sync.RWMutex
	db *leveldb.DB
}

// OpenStore opens or creates a node-local LevelDB database.
func OpenStore(path string) (*userTokenStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("open LevelDB: %w", err)
	}
	return &userTokenStore{db: db}, nil
}

// Lookup loads a token record by its key from this node's local database.
func (s *userTokenStore) Lookup(key string) (userTokenRecord, error) {
	if strings.TrimSpace(key) == "" {
		return userTokenRecord{}, errors.New("frontendTokenKey is required")
	}
	if s == nil {
		return userTokenRecord{}, errors.New("database is not open")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return userTokenRecord{}, errors.New("database is closed")
	}
	encoded, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return userTokenRecord{}, err
	}
	var record userTokenRecord
	if err := json.Unmarshal(encoded, &record); err != nil {
		return userTokenRecord{}, fmt.Errorf("decode token record: %w", err)
	}
	return record, nil
}

// ApplyIfNewer stores record only when it wins the deterministic last-write-
// wins ordering. It returns true when the local database was updated.
func (s *userTokenStore) ApplyIfNewer(record userTokenRecord) (bool, error) {
	if err := validateRecord(record); err != nil {
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
	currentBytes, err := s.db.Get(key, nil)
	if err == nil {
		var current userTokenRecord
		if err := json.Unmarshal(currentBytes, &current); err != nil {
			return false, fmt.Errorf("decode existing token record: %w", err)
		}
		if compareRecordVersions(record, current) <= 0 {
			return false, nil
		}
	} else if !errors.Is(err, leveldb.ErrNotFound) {
		return false, fmt.Errorf("read existing token record: %w", err)
	}

	encoded, err := json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("encode token record: %w", err)
	}
	if err := s.db.Put(key, encoded, &opt.WriteOptions{Sync: true}); err != nil {
		return false, fmt.Errorf("persist token record: %w", err)
	}
	return true, nil
}

// List returns all records in key order for initial synchronization with a peer.
func (s *userTokenStore) List() ([]userTokenRecord, error) {
	if s == nil {
		return nil, errors.New("database is not open")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return nil, errors.New("database is closed")
	}

	records := make([]userTokenRecord, 0)
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		var record userTokenRecord
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			iter.Release()
			return nil, fmt.Errorf("decode token record: %w", err)
		}
		records = append(records, record)
	}
	if err := iter.Error(); err != nil {
		iter.Release()
		return nil, fmt.Errorf("iterate token records: %w", err)
	}
	iter.Release()
	return records, nil
}

// Close releases the database handle. Repeated calls are safe.
func (s *userTokenStore) Close() error {
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

// validateRecord rejects incomplete and unversioned data before it is stored
// or accepted from a remote peer.
func validateRecord(record userTokenRecord) error {
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

// compareRecordVersions defines a total order: timestamp, origin peer ID, then
// canonical JSON. The final tie-break makes equal-time writes converge.
func compareRecordVersions(first, second userTokenRecord) int {
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
	firstBytes, _ := json.Marshal(first)
	secondBytes, _ := json.Marshal(second)
	switch {
	case string(firstBytes) > string(secondBytes):
		return 1
	case string(firstBytes) < string(secondBytes):
		return -1
	default:
		return 0
	}
}
