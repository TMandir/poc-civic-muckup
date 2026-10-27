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
	FrontendTokenKey string
	Token            string
	Information      string
	Message          string
	Found            bool
	FirstKey         string
	SecondKey        string
	Comparison       tokenComparison
	HasComparison    bool
	DiscussionResult string
}

func NewHandler(store *userTokenStore) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("user-token database is required")
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

		model := pageModel{FrontendTokenKey: r.URL.Query().Get("frontendTokenKey")}
		if r.URL.Query().Get("saved") == "1" {
			model.Message = "User token information saved."
		}
		if model.FrontendTokenKey != "" {
			record, err := store.Lookup(model.FrontendTokenKey)
			switch {
			case err == nil:
				model.Token = record.Token
				model.Information = record.Information
				model.Found = true
			case errors.Is(err, leveldb.ErrNotFound):
				model.Message = "No record was found for that frontendTokenKey."
			default:
				log.Printf("lookup user token for HTML page: %v", err)
				http.Error(w, "Unable to look up user token.", http.StatusInternalServerError)
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
					model.Message = "No record was found for one or both token keys."
				default:
					log.Printf("compare token values: %v", err)
					http.Error(w, "Unable to compare token values.", http.StatusInternalServerError)
					return
				}
			}
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := page.ExecuteTemplate(w, "index.html", model); err != nil {
			log.Printf("render user-token page: %v", err)
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

		record := userTokenRecord{
			FrontendTokenKey: r.FormValue("frontendTokenKey"),
			Token:            r.FormValue("token"),
			Information:      r.FormValue("information"),
		}
		if strings.TrimSpace(record.FrontendTokenKey) == "" ||
			strings.TrimSpace(record.Token) == "" ||
			strings.TrimSpace(record.Information) == "" {
			http.Error(w, "frontendTokenKey, token, and token information are required.", http.StatusBadRequest)
			return
		}
		if err := store.Save(record); err != nil {
			log.Printf("save user token: %v", err)
			http.Error(w, "Unable to save user token.", http.StatusInternalServerError)
			return
		}

		location := "/?saved=1&frontendTokenKey=" + url.QueryEscape(record.FrontendTokenKey)
		http.Redirect(w, r, location, http.StatusSeeOther)
	})

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
			return
		}
		frontendTokenKey := r.URL.Query().Get("frontendTokenKey")
		if strings.TrimSpace(frontendTokenKey) == "" {
			http.Error(w, "A frontendTokenKey query parameter is required.", http.StatusBadRequest)
			return
		}

		record, err := store.Lookup(frontendTokenKey)
		if errors.Is(err, leveldb.ErrNotFound) {
			http.Error(w, "No record was found for that frontendTokenKey.", http.StatusNotFound)
			return
		}
		if err != nil {
			log.Printf("lookup user token by frontendTokenKey: %v", err)
			http.Error(w, "Unable to look up user token.", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if err := json.NewEncoder(w).Encode(record); err != nil {
			log.Printf("encode user-token response: %v", err)
		}
	})

	return mux, nil
}
