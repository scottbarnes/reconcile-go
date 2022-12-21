package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

// Set some SQLite options, per https://avi.im/blag/2021/fast-sqlite-inserts/
// sqlite3 options at https://github.com/mattn/go-sqlite3#connection-string
const DBNAME string = "reconcile-go.db?_sync=0&_journal=WAL"

func main() {
	// Flags
	inFileOL := flag.String("oldump", "", "Open Library ALL dump file")
	flag.Parse()

	// Determine which infile to use for run().
	var inFile *string
	if *inFileOL != "" {
		inFile = inFileOL
	}

	// Run the actual program.
	if err := run(*inFile, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(inFile string, out io.Writer) error {
	// Create channels
	errCh := make(chan error)
	doneCh := make(chan struct{})
	editionsCh := make(chan *OpenLibraryEdition)

	// Get database
	db, err := getDB(DBNAME)
	if err != nil {
		return err
	}

	f, err := os.Open(inFile)
	if err != nil {
		return err
	}

	// Parse the file sending the results to editionsCh for database insertion.
	go func() {
		defer close(doneCh)
		defer f.Close()
		readFile(f, editionsCh, errCh)
	}()

	// Read from channels to get errors and recieve + insert DB items
	for {
		select {
		case err := <-errCh:
			return err
		case edition := <-editionsCh:
			// Could this use bundled []editions for performance?
			if edition == nil {
				continue
			}

			err := addEditionsToDB(edition, db)
			if err != nil {
				return err
			}
		case <-doneCh:
			// This must come last because this channel is closed in the file parser, but there will always be an edition until that's empty, so it can't get to the doneCh. I think.
			fmt.Fprintln(out, "All done.")
			return nil
		}
	}
}
