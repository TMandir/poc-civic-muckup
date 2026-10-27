package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	address := flag.String("addr", ":8081", "HTTP address for the example UI")
	databasePath := flag.String("db", "./example/uitodb/data", "LevelDB directory")
	flag.Parse()

	store, err := OpenStore(*databasePath)
	if err != nil {
		log.Fatalf("open token database: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close token database: %v", err)
		}
	}()

	handler, err := NewHandler(store)
	if err != nil {
		log.Fatalf("create example handler: %v", err)
	}

	log.Printf("serving keyed token example at %s", *address)
	if err := http.ListenAndServe(*address, handler); err != nil {
		log.Fatalf("serve keyed token example: %v", err)
	}
}
