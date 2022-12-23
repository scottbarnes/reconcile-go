package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

// Set some SQLite options, per https://avi.im/blag/2021/fast-sqlite-inserts/
// sqlite3 options at https://github.com/mattn/go-sqlite3#connection-string
const DBNAME string = "reconcile-go.db?_sync=0&_journal=WAL"

func main() {
	// Flags
	runType := flag.String("type", "", "Which iteration of run() to use")
	inFileOL := flag.String("oldump", "", "Open Library ALL dump file")
	flag.Parse()

	// Determine which infile to use for run().
	var inFile *string
	if *inFileOL != "" {
		inFile = inFileOL
	}

	switch *runType {
	case "original":
		// Run the actual program.
		if err := run(*inFile, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "runSeq":
		if err := runSeq(*inFile, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
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
		defer close(editionsCh)
		defer f.Close()
		err := readFile(f, editionsCh)
		if err != nil {
			errCh <- err
		}
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

			stmt, err := db.Prepare("INSERT INTO ol VALUES(NULL, ?, ?, ?)")
			if err != nil {
				return err
			}

			err = addEditionsToDB(edition, stmt)
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

func runSeq(inFile string, out io.Writer) error {
	f, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer f.Close()

	linesCh := make(chan []byte, 256)
	editionsCh := make(chan *OpenLibraryEdition, 256)
	errCh := make(chan error, 5)
	doneCh := make(chan struct{})
	wg := sync.WaitGroup{}

	go func() {
		defer close(linesCh)
		// Send lines straight to a channel for the parser(s) to pick up.
		const maxCapacity = 100 * 100 * 1000 // This size gets through the "ALL" dump.
		buf := make([]byte, maxCapacity)

		sc := bufio.NewScanner(f)
		sc.Buffer(buf, 1)
		for sc.Scan() {
			linesCh <- sc.Bytes()
		}
	}()

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for line := range linesCh {
				edition, err := parseOLLine(line)
				if err != nil {
					errCh <- fmt.Errorf("parsing error: %w", err)
				}
				editionsCh <- edition
			}
		}()
	}

	go func() {
		wg.Wait()
		close(doneCh)
	}()

	// This would be where they're inserted into the DB, but that's not relevant here.
	var editionCount int
	for {
		select {
		case err := <-errCh:
			fmt.Fprintln(out, err)
		case <-editionsCh:
			editionCount++
		case <-doneCh:
			fmt.Fprintln(out, "total count: ", editionCount)
			return nil
		}
	}
}
