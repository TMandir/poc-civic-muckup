package main

import (
	"embed"
	"encoding/json"
	"errors"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

//go:embed index.html
var pageFiles embed.FS

type pageModel struct {
	FrontendTokenKey string
	Token            string
	Information      string
	Message          string
	Found            bool
	FirstKey         string
	SecondKey        string
	Comparison       TokenComparison
	HasComparison    bool
	DiscussionResult string
	PeerID           string
	PeerAddresses    []string
	ConnectedPeers   []string
}

// NewHandler wires the server-rendered page and JSON endpoint to this node's
// local store and its peer network.
func NewHandler(store *userTokenStore, peers *peerNetwork) (http.Handler, error) {
	if store == nil || peers == nil {
		return nil, errors.New("database and peer network are required")
	}
	page, err := template.ParseFS(pageFiles, "index.html")
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
			return
		}

		model := pageModel{
			FrontendTokenKey: r.URL.Query().Get("frontendTokenKey"),
			PeerID:           peers.PeerID().String(),
			PeerAddresses:    peers.ListenAddresses(),
			ConnectedPeers:   peers.ConnectedPeers(),
		}
		if r.URL.Query().Get("saved") == "1" {
			model.Message = "Token saved locally and sent to connected peers."
		}
		if model.FrontendTokenKey != "" {
			record, err := store.Lookup(model.FrontendTokenKey)
			switch {
			case err == nil:
				model.Token = record.Token
				model.Information = record.Information
				model.Found = true
			case errors.Is(err, leveldb.ErrNotFound):
				model.Message = "No record was found for that token key on this node."
			default:
				log.Printf("lookup token for page: %v", err)
				http.Error(w, "Unable to look up token.", http.StatusInternalServerError)
				return
			}
		}
		if r.URL.Query().Get("compare") == "1" {
			model.FirstKey = r.URL.Query().Get("firstKey")
			model.SecondKey = r.URL.Query().Get("secondKey")
			if strings.TrimSpace(model.FirstKey) == "" || strings.TrimSpace(model.SecondKey) == "" {
				model.Message = "Enter both token keys to compare their values."
			} else {
				comparison, err := CompareTokenValues(store, model.FirstKey, model.SecondKey)
				switch {
				case err == nil:
					model.Comparison = comparison
					model.HasComparison = true
					model.DiscussionResult = ConcatenateTokenValues(comparison)
				case errors.Is(err, leveldb.ErrNotFound):
					model.Message = "One or both token keys were not found in this node's local database."
				default:
					log.Printf("compare local token values: %v", err)
					http.Error(w, "Unable to compare token values.", http.StatusInternalServerError)
					return
				}
			}
		}
		renderPage(w, http.StatusOK, model, page)
	})

	mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxPeerMessageBytes)
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Invalid form submission.", http.StatusBadRequest)
			return
		}
		record := userTokenRecord{
			FrontendTokenKey: strings.TrimSpace(r.FormValue("frontendTokenKey")),
			Token:            r.FormValue("token"),
			Information:      r.FormValue("information"),
			UpdatedAt:        time.Now().UTC(),
			Origin:           peers.PeerID().String(),
		}
		if err := validateRecord(record); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := store.ApplyIfNewer(record); err != nil {
			log.Printf("save local token: %v", err)
			http.Error(w, "Unable to save token.", http.StatusInternalServerError)
			return
		}
		if err := peers.Replicate(r.Context(), record); err != nil {
			model := pageModel{
				FrontendTokenKey: record.FrontendTokenKey,
				Message:          err.Error(),
				PeerID:           peers.PeerID().String(),
				PeerAddresses:    peers.ListenAddresses(),
				ConnectedPeers:   peers.ConnectedPeers(),
			}
			renderPage(w, http.StatusAccepted, model, page)
			return
		}
		http.Redirect(w, r, "/?saved=1&frontendTokenKey="+url.QueryEscape(record.FrontendTokenKey), http.StatusSeeOther)
	})

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("frontendTokenKey")
		if strings.TrimSpace(key) == "" {
			http.Error(w, "A frontendTokenKey query parameter is required.", http.StatusBadRequest)
			return
		}
		record, err := store.Lookup(key)
		if errors.Is(err, leveldb.ErrNotFound) {
			http.Error(w, "No record was found for that token key on this node.", http.StatusNotFound)
			return
		}
		if err != nil {
			log.Printf("lookup token JSON endpoint: %v", err)
			http.Error(w, "Unable to look up token.", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if err := json.NewEncoder(w).Encode(record); err != nil {
			log.Printf("encode token JSON response: %v", err)
		}
	})
	return mux, nil
}

// renderPage writes one server-rendered HTML response with its HTTP status.
func renderPage(w http.ResponseWriter, status int, model pageModel, page *template.Template) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := page.ExecuteTemplate(w, "index.html", model); err != nil {
		log.Printf("render token page: %v", err)
	}
}
