package database

import (
	"testing"
)

func TestStoreTokenPersistsAcrossReopen(t *testing.T) {
	path := t.TempDir()

	store, err := Open(path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := store.StoreToken("received-token"); err != nil {
		t.Fatalf("store token: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	defer store.Close()

	got, err := store.db.Get([]byte(frontendTokenKey), nil)
	if err != nil {
		t.Fatalf("read stored token: %v", err)
	}
	if string(got) != "received-token" {
		t.Fatalf("stored token = %q, want %q", got, "received-token")
	}

	count, err := store.CountTokens()
	if err != nil {
		t.Fatalf("count stored tokens: %v", err)
	}
	if count != 1 {
		t.Fatalf("stored token count = %d, want 1", count)
	}
}

func TestCountTokensTracksEverySubmission(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer store.Close()

	if count, err := store.CountTokens(); err != nil || count != 0 {
		t.Fatalf("initial token count = (%d, %v), want (0, nil)", count, err)
	}

	for want, token := range []string{"first-token", "second-token"} {
		if err := store.StoreToken(token); err != nil {
			t.Fatalf("store %q: %v", token, err)
		}
		count, err := store.CountTokens()
		if err != nil {
			t.Fatalf("count tokens after %q: %v", token, err)
		}
		if count != want+1 {
			t.Fatalf("token count after %q = %d, want %d", token, count, want+1)
		}
	}
}

func TestStoreTokenRejectsEmptyToken(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer store.Close()

	if err := store.StoreToken(" \t "); err == nil {
		t.Fatal("StoreToken accepted an empty token")
	}
}

func TestStoreTokenRejectsClosedDatabase(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	if err := store.StoreToken("received-token"); err == nil {
		t.Fatal("StoreToken succeeded after the database was closed")
	}
}

func TestGetTokenAndPop(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer store.Close()

	if token, found, err := store.GetToken(); err != nil || found || token != "" {
		t.Fatalf("GetToken on empty database = (%q, %t, %v), want empty, false, nil", token, found, err)
	}

	if err := store.Push("latest-token"); err != nil {
		t.Fatalf("push token: %v", err)
	}

	if token, found, err := store.GetToken(); err != nil || !found || token != "latest-token" {
		t.Fatalf("GetToken = (%q, %t, %v), want latest-token, true, nil", token, found, err)
	}

	if token, found, err := store.Pop(); err != nil || !found || token != "latest-token" {
		t.Fatalf("Pop = (%q, %t, %v), want latest-token, true, nil", token, found, err)
	}

	if token, found, err := store.GetToken(); err != nil || found || token != "" {
		t.Fatalf("GetToken after Pop = (%q, %t, %v), want empty, false, nil", token, found, err)
	}
}
