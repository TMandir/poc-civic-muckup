package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	address := flag.String("addr", ":8082", "HTTP address for the user-token example")
	databasePath := flag.String("db", "./example/user_tokens/data", "LevelDB directory")
	flag.Parse()

	store, err := OpenStore(*databasePath)
	if err != nil {
		log.Fatalf("open user-token database: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close user-token database: %v", err)
		}
	}()

	handler, err := NewHandler(store)
	if err != nil {
		log.Fatalf("create user-token handler: %v", err)
	}

	log.Printf("serving user-token example at %s", *address)
	if err := http.ListenAndServe(*address, handler); err != nil {
		log.Fatalf("serve user-token example: %v", err)
	}
}
