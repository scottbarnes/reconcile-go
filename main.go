package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
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
	case "runSeek":
		if err := runSeek(*inFile, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func runSeek(inFile string, out io.Writer) error {
	chunkSize := int64(1000 * 1000 * 1000)
	editionsCh := make(chan *OpenLibraryEdition, 256)
	errCh := make(chan error, 5)
	go tempAddEditionsToDB(editionsCh)
	if err := getEditions(inFile, out, editionsCh, errCh, chunkSize); err != nil {
		return err
	}

	return nil
}

func getEditions(inFile string, out io.Writer, editionsCh chan<- *OpenLibraryEdition, errCh chan error, chunkSize int64) error {
	chunksCh := make(chan *Chunk, 20)
	doneCh := make(chan struct{})
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
				// chunk.Print()
			}
		}()
	}

	// Once all the chunk.Process GoRoutines finish, no more editions
	// will be sent to editionsCh.
	go func() {
		wg.Wait()
		defer close(editionsCh)
		time.Sleep(200 * time.Millisecond)
		defer close(doneCh)
	}()

	// This would be where they're inserted into the DB, but that's not relevant here.
	// var editionCount int
	for {
		select {
		case err := <-errCh:
			fmt.Fprintln(out, err)
		// case <-editionsCh:
		// 	editionCount++
		case <-doneCh:
			// fmt.Fprintln(out, "total count: ", editionCount)
			return nil
		}
	}
}

func tempAddEditionsToDB(editionsCh <-chan *OpenLibraryEdition) {
	var totalEditions int
	// for edition := range editionsCh {
	for range editionsCh {
		totalEditions++
		// fmt.Println(edition.olid)
	}

	fmt.Println("Total editions:", totalEditions)
}
