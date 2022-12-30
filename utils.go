package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// getIsbn13CheckDigit calculates the check digit for an ISBN 13 based on the
// first twelve digits of the number. Works with a full ISBN 13 or just the
// first 12 digits.
// NOTE: This does *NOT* verify that the ISBN 10, and therefore ISBN 13, is valid,
// so it can produce invalid ISBN 13s based on invalid ISBN 10s.
func getIsbn13CheckDigit(isbn string) (string, error) {
	chars := strings.Split(isbn, "")
	chars = chars[:12]
	var checkDigit string
	var sum int

	// Formula adapted from xlcnd/isbnlib
	// https://github.com/xlcnd/isbnlib/blob/f4e7339ced8d42939318ce3adc7823a45fcd1c5b/isbnlib/_core.py#L77
	for i, v := range chars {

		val, err := strconv.Atoi(v)
		if err != nil {
			// Ignore errors that crop up from trying to convert characters such as "w" or "/".
			if errors.Is(err, strconv.ErrSyntax) {
				return "", nil
			}
			return "", err
		}

		sum += (i%2*2 + 1) * val
	}

	checkDigit = strconv.Itoa(10 - sum%10)
	if checkDigit == "10" {
		checkDigit = "0"
	}
	return checkDigit, nil
}

// getDB gets a SQLite DB based on the name, such as ":memory:".
func getDB(dbName string) (*sql.DB, error) {
	OLSCHEMA := `
  CREATE TABLE IF NOT EXISTS ol (
    id INTEGER NOT NULL PRIMARY KEY,
    edition_id text,
    ocaid text,
    isbn_13 text
  );`

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	// Initalize the DB if necessary.
	if _, err := db.Exec(OLSCHEMA); err != nil {
		return nil, err
	}

	return db, nil
}

// Chunk provides an interface for working with files in need of parsing.
type Chunk struct {
	filename string
	start    int64
	end      int64
	// Possibly add parserFunc, so the OL or IA parser func can be added.
}

func NewChunk(filename string, start int64, end int64) *Chunk {
	return &Chunk{
		filename: filename,
		start:    start,
		end:      end,
	}
}

func (c *Chunk) Process(editionsCh chan<- *OpenLibraryEdition, errCh chan<- error) {
	f, err := os.Open(c.filename)
	if err != nil {
		errCh <- err
		return
	}
	defer f.Close()

	f.Seek(c.start, 0)
	byteCount := int64(-1) // Fix off-by-one.

	sc := bufio.NewScanner(f)
	buf := make([]byte, 10*1000)
	sc.Buffer(buf, 10*1000*1000)
	for sc.Scan() {
		line := sc.Bytes()

		// Keep track of bytes read and exit once the number exceeds c.end.
		byteCount += int64(len(line) + 1)

		if c.start+byteCount > c.end {
			break
		}

		edition, err := parseOLLine(line)
		if err != nil {
			// if errors.Is(err, ErrorWrongColCount) || errors.Is(err, ErrorNotEdition) {
			if errors.Is(err, ErrorNotEdition) {
				continue
			} else {
				errCh <- err
			}
		}

		editionsCh <- edition
	}

	if err := sc.Err(); err != nil {
		errCh <- fmt.Errorf("error near byte: %v", byteCount)
		errCh <- fmt.Errorf("scanner error: %w", err)
	}
}

// Read a file and break it into chunks of start+end offsets in
// bytes so that the file can be read in chunks.
// Chunks start/end on a new line character.
func getChunks(chunkSize int64, filename string) ([]*Chunk, error) {
	chunks := []*Chunk{}
	readAhead := int64(10 * 1000)
	chunkEndOffset := int64(0)
	chunkStart := int64(0) // Gets value from previous chunkEndOffset
	readAheadBuf := make([]byte, readAhead)
	var fileEnd int64

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fstat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileEnd = fstat.Size()

	// Iterate through the file by chunkSize (plus a bit more to seek for
	// the newline). Do this by seeking to (near) the new chunkEnd, then
	// read a bit more and look for a newline.
	for {
		chunkEndOffset += chunkSize

		// At the end of the file, create the last chunk and break out of the loop.
		if chunkEndOffset >= fileEnd {
			chunk := NewChunk(filename, chunkStart, fileEnd)
			chunks = append(chunks, chunk)
			break
		}

		// Get into position to look for and of chunk newline by seeking forward
		// to the approximate end of the chunk.
		currentOffset, err := f.Seek(chunkEndOffset, 0)
		if err != nil {
			return nil, err
		}

		_, err = f.Read(readAheadBuf)
		if err != nil {
			return nil, err
		}

		// Find the next newline and use it to complete the new chunkEndOffset.
		// TODO: make it so this handles the case of a newline NOT being found
		// in readAheadBuf.
		for i := range readAheadBuf {
			if readAheadBuf[i] == '\n' {
				chunkEndOffset = currentOffset + int64(i)
				chunk := NewChunk(filename, chunkStart, chunkEndOffset)
				chunks = append(chunks, chunk)
				chunkStart = chunkEndOffset + 1 // start on the newline character.
				break
			}
		}
	}

	return chunks, nil
}
