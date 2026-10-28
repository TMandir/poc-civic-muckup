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
	older := Record{FrontendTokenKey: "key", Token: "older", Information: "info", UpdatedAt: at, Origin: "peer-a"}
	newer := Record{FrontendTokenKey: "key", Token: "newer", Information: "info", UpdatedAt: at.Add(time.Second), Origin: "peer-a"}
	if _, err := store.ApplyIfNewer(newer); err != nil {
		t.Fatalf("save newer record: %v", err)
	}
	if applied, err := store.ApplyIfNewer(older); err != nil || applied {
		t.Fatalf("apply older record = %v, %v; want false, nil", applied, err)
	}
	got, err := store.Lookup("key")
	if err != nil || got.Token != "newer" {
		t.Fatalf("lookup winner = %+v, %v", got, err)
	}
	records, err := store.List()
	if err != nil || len(records) != 1 {
		t.Fatalf("list snapshot = %d records, %v", len(records), err)
	}
	if _, err := store.Lookup("missing"); !errors.Is(err, leveldb.ErrNotFound) {
		t.Fatalf("missing lookup error = %v, want LevelDB not-found", err)
	}
}
