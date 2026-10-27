package frontend

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	board "whiteboard/Board"
)

type testTokenStore struct {
	mu    sync.Mutex
	token string
	found bool
	count int
}

func (s *testTokenStore) StoreToken(token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = token
	s.found = true
	s.count++
	return nil
}

func (s *testTokenStore) GetToken() (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token, s.found, nil
}

func (s *testTokenStore) CountTokens() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count, nil
}

func TestNewHandlerRendersHTMLAndSavedToken(t *testing.T) {
	store := &testTokenStore{token: "saved-token", found: true, count: 1}
	handler, err := NewHandler(store)
	if err != nil {
		t.Fatalf("create frontend handler: %v", err)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("GET / content type = %q, want HTML", got)
	}
	if !strings.Contains(response.Body.String(), "action=\"/ui/submit\"") {
		t.Fatal("HTML page does not include the native token submission form")
	}
	if !strings.Contains(response.Body.String(), "saved-token") {
		t.Fatal("HTML page does not display the stored token")
	}
	if !strings.Contains(response.Body.String(), "Total number of tokens entered in the whiteboard: <strong>1</strong>") {
		t.Fatal("HTML page does not display the total token count")
	}
}

func TestUISubmitStoresTokenAndRedirects(t *testing.T) {
	store := &testTokenStore{}
	handler, err := NewHandler(store)
	if err != nil {
		t.Fatalf("create frontend handler: %v", err)
	}

	forwarded := make(chan []byte, 1)
	go func() {
		forwarded <- <-board.ClientToServer
	}()

	form := url.Values{"bigtoken": {"submitted-from-html"}}
	request := httptest.NewRequest(
		http.MethodPost,
		"/ui/submit",
		strings.NewReader(form.Encode()),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusSeeOther {
		t.Fatalf("POST /ui/submit status = %d, want %d: %s", response.Code, http.StatusSeeOther, response.Body)
	}
	if got := response.Header().Get("Location"); got != "/?saved=1" {
		t.Fatalf("redirect location = %q, want /?saved=1", got)
	}

	var received board.Bigtoken
	if err := json.Unmarshal(<-forwarded, &received); err != nil {
		t.Fatalf("decode forwarded token: %v", err)
	}
	if received.BigToken != "submitted-from-html" {
		t.Fatalf("forwarded token = %q, want submitted-from-html", received.BigToken)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if !strings.Contains(response.Body.String(), "Total number of tokens entered in the whiteboard: <strong>1</strong>") {
		t.Fatal("HTML page does not display the updated token count")
	}
}

func TestSubmitTokenAcceptsXMLAndForwardsToken(t *testing.T) {
	store := &testTokenStore{}
	handler, err := NewHandler(store)
	if err != nil {
		t.Fatalf("create frontend handler: %v", err)
	}

	forwarded := make(chan []byte, 1)
	go func() {
		forwarded <- <-board.ClientToServer
	}()

	request := httptest.NewRequest(
		http.MethodPost,
		"/submit",
		strings.NewReader("<token><bigtoken>submitted-token</bigtoken></token>"),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("POST /submit status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body)
	}
	var result xmlToken
	if err := xml.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode XML submit response: %v", err)
	}
	if result.BigToken != "submitted-token" {
		t.Fatalf("POST /submit response token = %q, want submitted-token", result.BigToken)
	}

	var received board.Bigtoken
	if err := json.Unmarshal(<-forwarded, &received); err != nil {
		t.Fatalf("decode forwarded token: %v", err)
	}
	if received.BigToken != "submitted-token" {
		t.Fatalf("forwarded token = %q, want submitted-token", received.BigToken)
	}
}

func TestTokenReadRouteReturnsNotFoundWhenEmpty(t *testing.T) {
	handler, err := NewHandler(&testTokenStore{})
	if err != nil {
		t.Fatalf("create frontend handler: %v", err)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/token", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("GET /token status = %d, want %d", response.Code, http.StatusNotFound)
	}
	if got := response.Header().Get("Content-Type"); got != "application/xml; charset=utf-8" {
		t.Fatalf("GET /token content type = %q, want XML", got)
	}
}
