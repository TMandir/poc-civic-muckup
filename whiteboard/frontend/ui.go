package frontend

import (
	"embed"
	"errors"
	"html/template"
	"log"
	"net/http"
	"strings"
)

//go:embed UI/index.html
var uiFiles embed.FS

type uiPage struct {
	Token         string
	HasToken      bool
	TokenCount    int
	HasTokenCount bool
	Message       string
}

// registerUIRoutes wires the server-rendered HTML page and its native form endpoint.
func registerUIRoutes(mux *http.ServeMux, store TokenStore) error {
	page, err := template.ParseFS(uiFiles, "UI/index.html")
	if err != nil {
		return err
	}

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

		token, found, err := store.GetToken()
		if err != nil {
			log.Printf("Failed to load token for HTML page: %v", err)
			renderUI(w, http.StatusInternalServerError, uiPage{Message: "Unable to load the saved token."}, page)
			return
		}
		count, err := store.CountTokens()
		if err != nil {
			log.Printf("Failed to load token count for HTML page: %v", err)
			renderUI(w, http.StatusInternalServerError, uiPage{Message: "Unable to load the token count."}, page)
			return
		}
		model := uiPage{
			Token:         token,
			HasToken:      found,
			TokenCount:    count,
			HasTokenCount: true,
		}
		if r.URL.Query().Get("saved") == "1" {
			model.Message = "Token submitted and saved."
		}
		renderUI(w, http.StatusOK, model, page)
	})

	mux.HandleFunc("/ui/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "Method not allowed.", http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := r.ParseForm(); err != nil {
			renderUI(w, http.StatusBadRequest, uiPage{Message: "Invalid form submission."}, page)
			return
		}
		token := r.FormValue("bigtoken")
		if strings.TrimSpace(token) == "" {
			renderUI(w, http.StatusBadRequest, uiPage{Message: "Token is required."}, page)
			return
		}

		if err := store.StoreToken(token); err != nil {
			log.Printf("Failed to store token from HTML form: %v", err)
			renderUI(w, http.StatusInternalServerError, uiPage{Message: "Unable to save the token."}, page)
			return
		}
		if err := forwardToken(r, token); err != nil {
			if errors.Is(err, errRequestCanceled) {
				return
			}
			log.Printf("Failed to forward token from HTML form: %v", err)
			renderUI(w, http.StatusInternalServerError, uiPage{Message: "Token was saved but could not be forwarded."}, page)
			return
		}

		http.Redirect(w, r, "/?saved=1", http.StatusSeeOther)
	})

	return nil
}

// renderUI writes the HTML page with the supplied status and view data.
func renderUI(w http.ResponseWriter, status int, model uiPage, page *template.Template) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := page.ExecuteTemplate(w, "index.html", model); err != nil {
		log.Printf("Failed to render HTML page: %v", err)
	}
}
