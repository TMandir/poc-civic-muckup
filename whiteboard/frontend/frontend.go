// Package frontend provides an HTML token page and XML endpoints for submitting
// and retrieving the latest token. Submitted tokens are persisted and forwarded
// to the server.
package frontend

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	board "whiteboard/Board"
)

var errRequestCanceled = errors.New("request canceled")

// TokenStore is the persistence capability required by the submission handler.
// StoreToken persists a submitted value; GetToken retrieves the latest value.
type TokenStore interface {
	StoreToken(token string) error
	GetToken() (string, bool, error)
	CountTokens() (int, error)
}

// NewHandler creates the HTML page and XML API routes.
// The HTML form posts URL-encoded data to /ui/submit; the XML API uses /submit
// and /token.
func NewHandler(store TokenStore) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("token store is required")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/submit", submitToken(store))
	registerTokenReadRoute(mux, store)
	if err := registerUIRoutes(mux, store); err != nil {
		return nil, err
	}

	return mux, nil
}

// submitToken builds the HTTP endpoint around its persistence dependency.
// A successful request must contain one XML token document with a non-blank value.
func submitToken(store TokenStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeXMLError(w, http.StatusMethodNotAllowed, "Method not allowed.")
			return
		}

		// Cap the input before decoding so clients cannot submit unbounded bodies.
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		decoder := xml.NewDecoder(r.Body)
		var token xmlToken
		if err := decoder.Decode(&token); err != nil {
			writeXMLError(w, http.StatusBadRequest, "Invalid XML token request.")
			return
		}
		if strings.TrimSpace(token.BigToken) == "" {
			writeXMLError(w, http.StatusBadRequest, "Token is required.")
			return
		}

		var trailing xmlToken
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			writeXMLError(w, http.StatusBadRequest, "Invalid XML token request.")
			return
		}

		if err := store.StoreToken(token.BigToken); err != nil {
			log.Printf("Failed to store frontend token: %v", err)
			writeXMLError(w, http.StatusInternalServerError, "Failed to store token.")
			return
		}

		if err := forwardToken(r, token.BigToken); err != nil {
			log.Printf("Failed to forward frontend token: %v", err)
			if errors.Is(err, errRequestCanceled) {
				writeXMLError(w, http.StatusRequestTimeout, "Request was canceled before the token could be forwarded.")
				return
			}
			writeXMLError(w, http.StatusInternalServerError, "Failed to forward token.")
			return
		}

		writeXML(w, http.StatusAccepted, token)
	}
}

// forwardToken sends the accepted token to the in-process server channel.
func forwardToken(r *http.Request, token string) error {
	payload, err := json.Marshal(board.Bigtoken{BigToken: token})
	if err != nil {
		return err
	}

	select {
	case board.ClientToServer <- payload:
		return nil
	case <-r.Context().Done():
		return errRequestCanceled
	}
}

// xmlToken is the XML representation accepted and returned by the HTTP API.
type xmlToken struct {
	XMLName  xml.Name `xml:"token"`
	BigToken string   `xml:"bigtoken"`
}

// writeXML sets an XML content type and encodes a response with its HTTP status.
func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	if err := xml.NewEncoder(w).Encode(value); err != nil {
		log.Printf("Failed to encode XML response: %v", err)
	}
}

// writeXMLError returns a consistent XML error document for API failures.
func writeXMLError(w http.ResponseWriter, status int, message string) {
	type errorResponse struct {
		XMLName xml.Name `xml:"error"`
		Message string   `xml:"message"`
	}
	writeXML(w, status, errorResponse{Message: message})
}
