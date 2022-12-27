package main

import (
	"bufio"
	"bytes"
	"errors"
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
	case "runSeek":
		if err := runSeek(*inFile, os.Stdout); err != nil {
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
		// buf := make([]byte, 0, 1024*1024)
		sc := bufio.NewScanner(f)
		// sc.Buffer(buf, 100*1024*1024)
		totalLinesSentToChan := 0

		for sc.Scan() {
			l := sc.Bytes()
			columns := bytes.Split(l, []byte("\t"))
			// if len(columns) != 5 {
			// 	fmt.Println("inital bad line", columns)
			// }
			if len(columns) != 5 {
				errCh <- fmt.Errorf("%v, %w", string(columns[0]), ErrorWrongColCount)
				continue
			}

			editionType := []byte{47, 116, 121, 112, 101, 47, 101, 100, 105, 116, 105, 111, 110}
			if res := bytes.Compare(columns[0], editionType); res != 0 {
				errCh <- ErrorNotEdition
				continue
			}

			if err := sc.Err(); err != nil {
				errCh <- fmt.Errorf("scanner error: %w", err)
				continue
			}

			blob := columns[4]

			linesCh <- blob
			totalLinesSentToChan++
		}

		fmt.Println("total lines sent to channel", totalLinesSentToChan)
	}()

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Continually fetch lines packets and iterate through them.
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

func (c *Chunk) Process(editionCh chan<- *OpenLibraryEdition, errCh chan<- error) {
	f, err := os.Open(c.filename)
	if err != nil {
		errCh <- err
		return
	}
	defer f.Close()

	f.Seek(c.start, 0)
	var byteCount int64

	sc := bufio.NewScanner(f)
	buf := make([]byte, 1000*1000)
	sc.Buffer(buf, 1)
	for sc.Scan() {
		// buf = nil
		line := sc.Bytes()
		// r := bufio.NewReader(f)
		// r.Reset(r)
		// for {

		// Keep track of bytes read and exit once the number exceeds c.end.
		byteCount += int64(len(line))
		if c.start+byteCount > c.end {
			fmt.Println("breaking at chunk limit")
			break
		}

		if err := sc.Err(); err != nil {
			errCh <- fmt.Errorf("scanner error: %w", err)
			continue
		}
		// line, err := r.ReadBytes('\n')
		// if err != nil {
		// 	if err == io.EOF {
		// 		fmt.Println("Hit end of file")
		// 		return
		// 	}

		// 	errCh <- err
		// 	continue
		// }

		edition, err := parseOLLine(line)
		if err != nil {
			// if errors.Is(err, ErrorWrongColCount) || errors.Is(err, ErrorNotEdition) {
			if errors.Is(err, ErrorNotEdition) {
				continue
			} else {
				errCh <- err
			}
			// }
		}

		editionCh <- edition
	}
}

func runSeek(inFile string, out io.Writer) error {
	chunksCh := make(chan *Chunk, 20)
	editionsCh := make(chan *OpenLibraryEdition, 256)
	errCh := make(chan error, 5)
	doneCh := make(chan struct{})
	wg := sync.WaitGroup{}

	f, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer f.Close()

	chunks, err := getChunks(CHUNKSIZE, inFile)
	if err != nil {
		return err
	}
	fmt.Println("chunks are: ", chunks)

	go func() {
		for _, chunk := range chunks {
			fmt.Println(chunk)
			chunksCh <- chunk
		}
		defer close(chunksCh)
	}()

	// Spin up one GoRoutine per processor and grab chunks until they're gone.
	for i := 0; i < runtime.NumCPU(); i++ {
		// for i := 0; i < 2; i++ {
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
		time.Sleep(5 * time.Second)
		defer close(doneCh)
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
