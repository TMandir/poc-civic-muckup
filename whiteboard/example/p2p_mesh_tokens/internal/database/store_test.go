package database

import (
	"errors"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

func TestStoreAppliesNewestRecordAndListsSnapshot(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	at := time.Now().UTC()
	older := Record{FrontendTokenKey: "key", Token: "old", Information: "info", UpdatedAt: at, Origin: "peer-a"}
	newer := Record{FrontendTokenKey: "key", Token: "new", Information: "info", UpdatedAt: at.Add(time.Second), Origin: "peer-a"}
	if _, err := store.ApplyIfNewer(newer); err != nil {
		t.Fatalf("save newer version: %v", err)
	}
	if applied, err := store.ApplyIfNewer(older); err != nil || applied {
		t.Fatalf("apply older version = %v, %v; want false, nil", applied, err)
	}
	got, err := store.Lookup("key")
	if err != nil {
		t.Fatalf("lookup winner: %v", err)
	}
	if got.Token != "new" {
		t.Fatalf("winning token = %q, want new", got.Token)
	}
	records, err := store.List()
	if err != nil {
		t.Fatalf("list snapshot: %v", err)
	}
	if len(records) != 1 || records[0].FrontendTokenKey != "key" {
		t.Fatalf("snapshot records = %+v", records)
	}
	if _, err := store.Lookup("missing"); !errors.Is(err, leveldb.ErrNotFound) {
		t.Fatalf("missing lookup error = %v, want LevelDB not-found", err)
	}
}
