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

	"github.com/syndtr/goleveldb/leveldb"
)

//go:embed index.html
var pageFiles embed.FS

type pageModel struct {
	Key     string
	Value   string
	Message string
}

func NewHandler(store *tokenStore) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("token database is required")
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

		model := pageModel{Key: r.URL.Query().Get("key")}
		if r.URL.Query().Get("saved") == "1" {
			model.Message = "Key and token value saved."
		}
		if model.Key != "" {
			record, err := store.Lookup(model.Key)
			switch {
			case err == nil:
				model.Value = record.Value
			case errors.Is(err, leveldb.ErrNotFound):
				model.Message = "No token is stored for that key."
			default:
				log.Printf("lookup token for HTML page: %v", err)
				http.Error(w, "Unable to look up token.", http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := page.ExecuteTemplate(w, "index.html", model); err != nil {
			log.Printf("render token page: %v", err)
		}
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

		key := r.FormValue("key")
		value := r.FormValue("value")
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			http.Error(w, "Both key and token value are required.", http.StatusBadRequest)
			return
		}
		if err := store.Save(key, value); err != nil {
			log.Printf("save token: %v", err)
			http.Error(w, "Unable to save token.", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/?saved=1&key="+url.QueryEscape(key), http.StatusSeeOther)
	})

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if strings.TrimSpace(key) == "" {
			http.Error(w, "A key query parameter is required.", http.StatusBadRequest)
			return
		}
		record, err := store.Lookup(key)
		if errors.Is(err, leveldb.ErrNotFound) {
			http.Error(w, "No token is stored for that key.", http.StatusNotFound)
			return
		}
		if err != nil {
			log.Printf("lookup token by key: %v", err)
			http.Error(w, "Unable to look up token.", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if err := json.NewEncoder(w).Encode(record); err != nil {
			log.Printf("encode token response: %v", err)
		}
	})

	return mux, nil
}
