// Package client contains the prototype peer that exchanges token messages
// with the server through the shared board channels.
package client

import (
	"encoding/json"
	"fmt"
	board "whiteboard/Board"
)

// Client starts the prototype client's sender and receiver goroutines.
func Client() {
	go forwardentity()
	go receiveentity()
}

// forwardentity marshals a placeholder token and sends it to the server.
func forwardentity() {
	forwarding_token := board.Bigtoken{
		BigToken: "This is the entity to be forwarded to the server from client",
	}

	payload, err := json.Marshal(forwarding_token)
	if err != nil {
		fmt.Println("Failed to encode bigtoken:", err)
		return
	}
	board.ClientToServer <- payload
}

// receiveentity reads one JSON token message from the server and logs its value.
func receiveentity() {
	payload := <-board.ServerToClient
	var token board.Bigtoken
	if err := json.Unmarshal(payload, &token); err != nil {
		fmt.Println("Failed to decode bigtoken:", err)
		return
	}
	fmt.Println("Received bigtoken from server in client:", token.BigToken)
}
