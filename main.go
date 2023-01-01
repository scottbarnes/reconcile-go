package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

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
	case "runSeek":
		if err := runSeek(*inFile, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func runSeek(inFile string, out io.Writer) error {
	chunkSize := int64(1000 * 1000 * 1000)
	doneCh := make(chan struct{})
	editionsCh := make(chan *OpenLibraryEdition, 256)
	errCh := make(chan error, 5)
	dbName := DBNAME

	// Get a DB
	db, err := getDB(dbName)
	if err != nil {
		return err
	}

	// Add editions from editionsCh
	go func() {
		addEditionToDBBatch(editionsCh, doneCh, db, 250)
	}()

	if err := getEditions(inFile, out, editionsCh, doneCh, errCh, chunkSize); err != nil {
		return err
	}

	// Block until done
	<-doneCh

	return nil
}

func getEditions(inFile string, out io.Writer, editionsCh chan<- *OpenLibraryEdition, doneCh <-chan struct{}, errCh chan error, chunkSize int64) error {
	chunksCh := make(chan *Chunk, 20)
	// doneCh := make(chan struct{})
	wg := sync.WaitGroup{}

	f, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer f.Close()

	chunks, err := getChunks(chunkSize, inFile)
	if err != nil {
		return err
	}

	go func() {
		for _, chunk := range chunks {
			chunksCh <- chunk
		}
		defer close(chunksCh)
	}()

	// Spin up one GoRoutine per processor and grab chunks until they're gone.
	for i := 0; i < runtime.NumCPU(); i++ {
		// for i := 0; i < 1; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Each GoRoutine grabs chunks until there are no more.
			for chunk := range chunksCh {
				chunk.Process(editionsCh, errCh)
			}
		}()
	}

	// Once all the chunk.Process GoRoutines finish, no more editions
	// will be sent to editionsCh. Closing the channel tells addEditionToDBBatch
	// that there are no more editions to add to the DB.
	go func() {
		wg.Wait()
		defer close(editionsCh)
	}()

	// This would be where they're inserted into the DB, but that's not relevant here.
	// var editionCount int
	for {
		select {
		case err := <-errCh:
			fmt.Fprintln(out, err)
		case <-doneCh:
			return nil
		}
	}
}
