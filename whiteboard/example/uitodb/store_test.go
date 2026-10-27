package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestStoreSavesAndLooksUpJSONRecord(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	if err := store.Save("agent-1", "token-value"); err != nil {
		t.Fatalf("save token: %v", err)
	}
	record, err := store.Lookup("agent-1")
	if err != nil {
		t.Fatalf("lookup token: %v", err)
	}
	if record.Key != "agent-1" || record.Value != "token-value" {
		t.Fatalf("lookup = (%q, %q), want (%q, %q)", record.Key, record.Value, "agent-1", "token-value")
	}
}

func TestHandlerSavesFromFormAndLooksUpByKey(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	handler, err := NewHandler(store)
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}

	form := url.Values{"key": {"agent-2"}, "value": {"submitted-token"}}
	request := httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST /submit status = %d, want %d", response.Code, http.StatusSeeOther)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/token?key=agent-2", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /token status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	var record tokenRecord
	if err := json.Unmarshal(response.Body.Bytes(), &record); err != nil {
		t.Fatalf("decode token response: %v", err)
	}
	if record.Key != "agent-2" || record.Value != "submitted-token" {
		t.Fatalf("GET /token = (%q, %q), want (%q, %q)", record.Key, record.Value, "agent-2", "submitted-token")
	}
}
