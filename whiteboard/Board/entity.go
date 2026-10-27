// Package board defines the token message format and in-process message channels
// shared by the frontend, server, and client packages.
package board

// ServerToClient carries JSON-encoded tokens from the server to the client.
var ServerToClient = make(chan []byte)

// ClientToServer carries JSON-encoded tokens from the frontend or client to the server.
var ClientToServer = make(chan []byte)

// Bigtoken is the JSON message exchanged by the frontend, server, and client.
// BigToken contains the submitted token value and is serialized as "bigtoken".
type Bigtoken struct {
	BigToken string `json:"bigtoken"`
}
