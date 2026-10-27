// Package Server contains prototype server routines that route token messages
// between the client and the shared in-process channels.
package Server

import (
	"encoding/json"
	"fmt"
	board "whiteboard/Board"
)

// Server starts the token sender and the long-running client-message receiver.
func Server() {
	go forwardentity()
	go receiveentity()
}

// forwardentity marshals a placeholder token and sends it to the client.
func forwardentity() {
	forwarding_token := board.Bigtoken{
		BigToken: "This is the entity to be forwarded to the client from server",
	}

	payload, err := json.Marshal(forwarding_token)
	if err != nil {
		fmt.Println("Failed to encode bigtoken:", err)
		return
	}
	board.ServerToClient <- payload
}

// receiveentity decodes and logs every token received from the client channel.
func receiveentity() {
	for receivedToken := range board.ClientToServer {
		var token board.Bigtoken
		if err := json.Unmarshal(receivedToken, &token); err != nil {
			fmt.Println("Failed to decode bigtoken from client:", err)
			continue
		}
		fmt.Println("Received the entity from the client:", token.BigToken)
	}
}
