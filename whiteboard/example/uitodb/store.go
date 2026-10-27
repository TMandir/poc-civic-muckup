package main

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type tokenRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type tokenStore struct {
	mu sync.RWMutex
	db *leveldb.DB
}

func OpenStore(path string) (*tokenStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &tokenStore{db: db}, nil
}

func (s *tokenStore) Save(key, value string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("key is required")
	}
	if strings.TrimSpace(value) == "" {
		return errors.New("token value is required")
	}
	if s == nil {
		return errors.New("database is not open")
	}

	record, err := json.Marshal(tokenRecord{Key: key, Value: value})
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return errors.New("database is closed")
	}
	return s.db.Put([]byte(key), record, &opt.WriteOptions{Sync: true})
}

func (s *tokenStore) Lookup(key string) (tokenRecord, error) {
	if strings.TrimSpace(key) == "" {
		return tokenRecord{}, errors.New("key is required")
	}
	if s == nil {
		return tokenRecord{}, errors.New("database is not open")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return tokenRecord{}, errors.New("database is closed")
	}

	value, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return tokenRecord{}, err
	}
	var record tokenRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return tokenRecord{}, err
	}
	return record, nil
}

func (s *tokenStore) Close() error {
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
