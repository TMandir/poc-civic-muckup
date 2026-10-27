package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestSaveAndLookupByFrontendTokenKey(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	want := userTokenRecord{
		FrontendTokenKey: "user-123",
		Token:            "sample-token",
		Information:      "Proof of it is You",
	}
	if err := store.Save(want); err != nil {
		t.Fatalf("save record: %v", err)
	}
	got, err := store.Lookup(want.FrontendTokenKey)
	if err != nil {
		t.Fatalf("lookup record: %v", err)
	}
	if got != want {
		t.Fatalf("lookup = %+v, want %+v", got, want)
	}
}

func TestHandlerSubmitsAndRetrievesUserRecord(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	handler, err := NewHandler(store)
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}

	form := url.Values{
		"frontendTokenKey": {"user-456"},
		"token":            {"another-token"},
		"information":      {"Proof of it is You"},
	}
	request := httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST /submit status = %d, want %d: %s", response.Code, http.StatusSeeOther, response.Body)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/token?frontendTokenKey=user-456", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /token status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	var got userTokenRecord
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.FrontendTokenKey != "user-456" || got.Token != "another-token" || got.Information != "Proof of it is You" {
		t.Fatalf("GET /token record = %+v, unexpected", got)
	}
}

func TestHandlerRendersTokenComparison(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	for _, record := range []userTokenRecord{
		{FrontendTokenKey: "user-1", Token: "one", Information: "first"},
		{FrontendTokenKey: "user-2", Token: "two-token", Information: "second"},
	} {
		if err := store.Save(record); err != nil {
			t.Fatalf("save record for %q: %v", record.FrontendTokenKey, err)
		}
	}

	handler, err := NewHandler(store)
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/?compare=1&firstKey=user-1&secondKey=user-2", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("GET comparison status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	for _, expected := range []string{
		"First token key",
		"Second token key",
		"Key: <code>user-1</code>",
		"one</pre>",
		"Value length: <strong>3</strong> characters",
		"Key: <code>user-2</code>",
		"two-token</pre>",
		"Value length: <strong>9</strong> characters",
		"<h3 id=\"discussion-result-heading\">Discussion Result</h3>",
		"<pre>onetwo-token</pre>",
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("comparison page missing %q", expected)
		}
	}
}
