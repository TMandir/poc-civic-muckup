package main

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type userTokenRecord struct {
	FrontendTokenKey string `json:"frontendTokenKey"`
	Token            string `json:"token"`
	Information      string `json:"information"`
}

type userTokenStore struct {
	mu sync.RWMutex
	db *leveldb.DB
}

func OpenStore(path string) (*userTokenStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &userTokenStore{db: db}, nil
}

func (s *userTokenStore) Save(record userTokenRecord) error {
	if strings.TrimSpace(record.FrontendTokenKey) == "" {
		return errors.New("frontendTokenKey is required")
	}
	if strings.TrimSpace(record.Token) == "" {
		return errors.New("token is required")
	}
	if strings.TrimSpace(record.Information) == "" {
		return errors.New("token information is required")
	}
	if s == nil {
		return errors.New("database is not open")
	}

	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		return errors.New("database is closed")
	}
	return s.db.Put([]byte(record.FrontendTokenKey), encoded, &opt.WriteOptions{Sync: true})
}

func (s *userTokenStore) Lookup(frontendTokenKey string) (userTokenRecord, error) {
	if strings.TrimSpace(frontendTokenKey) == "" {
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

	encoded, err := s.db.Get([]byte(frontendTokenKey), nil)
	if err != nil {
		return userTokenRecord{}, err
	}
	var record userTokenRecord
	if err := json.Unmarshal(encoded, &record); err != nil {
		return userTokenRecord{}, err
	}
	return record, nil
}

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
