package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"cpa-session-archive/internal/archive"
)

func main() {
	dbPath := flag.String("db", "", "archive SQLite database")
	filePath := flag.String("file", "", "JSON mapping file (mappings or key-policy credentials envelope)")
	flag.Parse()
	if *dbPath == "" || *filePath == "" {
		fmt.Fprintln(os.Stderr, "-db and -file are required")
		os.Exit(2)
	}
	raw, err := os.ReadFile(*filePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	items, err := archive.DecodeCredentialPrincipals(raw)
	if err != nil || len(items) == 0 {
		fmt.Fprintln(os.Stderr, "mapping file is invalid or empty")
		os.Exit(1)
	}
	store, err := archive.OpenStore(*dbPath, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer store.DB.Close()
	if err = store.ApplyCredentialPrincipals(context.Background(), items); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("updated %d credential mappings\n", len(items))
}
