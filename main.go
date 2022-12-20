package main

import (
	"flag"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3" // See https://earthly.dev/blog/golang-sqlite/ for an explanation of this side effect.
)

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
	if err := run(*inFile); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(inFile string) error {
	// Create channels
	errCh := make(chan error)
	doneCh := make(chan struct{})
	editionsCh := make(chan *OpenLibraryEdition)

	// wg := sync.WaitGroup{}

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
	go readFile(f, editionsCh, errCh, doneCh)

	// Read from channels to get errors and recieve + insert DB items
	for {
		select {
		case err := <-errCh:
			// return err
			fmt.Println(err)
		case edition := <-editionsCh:
			// Could this use bundled []editions for performance?
			addEditionsToDB(edition, db, errCh)
			// This must come last because this channel is closed in the file parser, but there will always be an edition until that's empty, so it can't get to the doneCh. I think.
		case <-doneCh:
			f.Close()
			fmt.Println("All done.")
			return nil
		}
	}
}
