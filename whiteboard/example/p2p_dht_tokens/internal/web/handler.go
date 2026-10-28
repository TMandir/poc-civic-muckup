// Package web implements the local browser UI for one P2P token database.
package web

import (
	"embed"
	"errors"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"sunya-p2p-dht-tokens/internal/compare"
	"sunya-p2p-dht-tokens/internal/database"
	"sunya-p2p-dht-tokens/internal/peer"
)

//go:embed index.html
var pageFiles embed.FS

// pageModel contains values rendered by the server-side HTML template.
type pageModel struct {
	Key            string
	Token          string
	Information    string
	Message        string
	Found          bool
	FirstKey       string
	SecondKey      string
	Comparison     compare.Result
	HasComparison  bool
	Combined       string
	PeerID         string
	Addresses      []string
	ConnectedPeers []string
}

// NewHandler creates routes backed by the local database and discovered peers.
func NewHandler(store *database.Store, network *peer.Network) (http.Handler, error) {
	if store == nil || network == nil {
		return nil, errors.New("database and P2P network are required")
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
			Key:            r.URL.Query().Get("key"),
			PeerID:         network.ID().String(),
			Addresses:      network.Addresses(),
			ConnectedPeers: network.ConnectedPeers(),
		}
		if r.URL.Query().Get("saved") == "1" {
			model.Message = "Record saved and sent to connected peers."
		}
		if model.Key != "" {
			record, err := store.Lookup(model.Key)
			switch {
			case err == nil:
				model.Token, model.Information, model.Found = record.Token, record.Information, true
			case errors.Is(err, leveldb.ErrNotFound):
				model.Message = "No record found in this node's local database."
			default:
				log.Printf("lookup token for page: %v", err)
				http.Error(w, "Unable to look up token.", http.StatusInternalServerError)
				return
			}
		}
		if r.URL.Query().Get("compare") == "1" {
			model.FirstKey, model.SecondKey = r.URL.Query().Get("firstKey"), r.URL.Query().Get("secondKey")
			result, err := compare.CompareTokenValues(store, model.FirstKey, model.SecondKey)
			switch {
			case err == nil:
				model.Comparison, model.HasComparison = result, true
				model.Combined = compare.ConcatenateTokenValues(result)
			case errors.Is(err, leveldb.ErrNotFound):
				model.Message = "One or both keys are not in this node's local database yet."
			default:
				log.Printf("compare local token records: %v", err)
				http.Error(w, "Unable to compare token values.", http.StatusInternalServerError)
				return
			}
		}
		render(w, http.StatusOK, model, page)
	})

	mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Invalid form submission.", http.StatusBadRequest)
			return
		}
		record := database.Record{
			FrontendTokenKey: strings.TrimSpace(r.FormValue("key")),
			Token:            r.FormValue("token"),
			Information:      r.FormValue("information"),
			UpdatedAt:        time.Now().UTC(),
			Origin:           network.ID().String(),
		}
		if err := database.Validate(record); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := store.ApplyIfNewer(record); err != nil {
			log.Printf("save local token: %v", err)
			http.Error(w, "Unable to save token.", http.StatusInternalServerError)
			return
		}
		if err := network.Broadcast(r.Context(), record); err != nil {
			render(w, http.StatusAccepted, pageModel{
				Key: record.FrontendTokenKey, Message: err.Error(),
				PeerID: network.ID().String(), Addresses: network.Addresses(),
				ConnectedPeers: network.ConnectedPeers(),
			}, page)
			return
		}
		http.Redirect(w, r, "/?saved=1&key="+url.QueryEscape(record.FrontendTokenKey), http.StatusSeeOther)
	})
	return mux, nil
}

// render writes a server-rendered page with the supplied status code.
func render(w http.ResponseWriter, status int, model pageModel, page *template.Template) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := page.ExecuteTemplate(w, "index.html", model); err != nil {
		log.Printf("render token UI: %v", err)
	}
}
