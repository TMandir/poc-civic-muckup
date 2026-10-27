package frontend

import (
	"log"
	"net/http"
)

// registerTokenReadRoute exposes the latest persisted token as XML.
// GET /token returns a token document when present and XML errors otherwise.
func registerTokenReadRoute(mux *http.ServeMux, store TokenStore) {
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeXMLError(w, http.StatusMethodNotAllowed, "Method not allowed.")
			return
		}

		token, found, err := store.GetToken()
		if err != nil {
			log.Printf("Failed to load frontend token: %v", err)
			writeXMLError(w, http.StatusInternalServerError, "Failed to load token.")
			return
		}
		if !found {
			writeXMLError(w, http.StatusNotFound, "No token has been saved.")
			return
		}

		writeXML(w, http.StatusOK, xmlToken{BigToken: token})
	})
}
