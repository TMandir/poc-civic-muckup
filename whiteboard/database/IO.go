package database

import (
	"errors"
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Push stores a token as the latest frontend token.
func (d *Database) Push(token string) error {
	return d.StoreToken(token)
}

// GetToken returns the latest token and whether a value has been stored.
func (d *Database) GetToken() (string, bool, error) {
	if d == nil {
		return "", false, errors.New("database is not open")
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.db == nil {
		return "", false, errors.New("database is closed")
	}

	value, err := d.db.Get([]byte(frontendTokenKey), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	return string(value), true, nil
}

// CountTokens returns the total number of tokens submitted to the database.
func (d *Database) CountTokens() (int, error) {
	if d == nil {
		return 0, errors.New("database is not open")
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.db == nil {
		return 0, errors.New("database is closed")
	}

	return d.tokenCountLocked()
}

// tokenCountLocked reads the count while the caller holds d.mu.
func (d *Database) tokenCountLocked() (int, error) {
	value, err := d.db.Get([]byte(tokenCountKey), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		found, hasErr := d.db.Has([]byte(frontendTokenKey), nil)
		if hasErr != nil {
			return 0, hasErr
		}
		if found {
			return 1, nil
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	count, err := strconv.Atoi(string(value))
	if err != nil || count < 0 {
		return 0, errors.New("invalid token count in database")
	}
	return count, nil
}

// Pop retrieves and deletes the latest token. It reports found=false when the
// database contains no token; database errors are returned without being hidden.
func (d *Database) Pop() (string, bool, error) {
	if d == nil {
		return "", false, errors.New("database is not open")
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db == nil {
		return "", false, errors.New("database is closed")
	}

	value, err := d.db.Get([]byte(frontendTokenKey), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	if err := d.db.Delete([]byte(frontendTokenKey), &opt.WriteOptions{Sync: true}); err != nil {
		return "", false, err
	}

	return string(value), true, nil
}
