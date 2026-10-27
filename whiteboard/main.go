// Package main is the executable entry point for the whiteboard application.
package main

import (
	"log"

	"whiteboard/app"
)

// main delegates startup, signal handling, and shutdown to the app package.
func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
