package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/buger/jsonparser"
	_ "github.com/mattn/go-sqlite3" // See http://go-database-sql.org/importing.html for an explanation of this side effect.
)

// For each line
// - run unmasher method on OpenLibraryEdition struct.
// - send struct to channel.

const PREFIX string = "978"

type OpenLibraryEdition struct {
	olid   string
	ocaid  string
	isbn10 string
	isbn13 string
}

// paths is a list of JSON paths parsed by buger/jasonparser.
var paths = [][]string{
	{"key"},
	{"ocaid"},
	{"isbn_10"},
	{"isbn_13"},
}

// toIsbn13 converts an *OpenLibraryEdition isbn10 to ISBN 13 and sets isbn13.
func (o *OpenLibraryEdition) toIsbn13() error {
	// Set ISBNs that aren't 10 characters to 0000000000 for easy identification.
	if len(o.isbn10) != 10 {
		o.isbn10 = "0000000000"
	}

	firstNine := o.isbn10[:9]
	firstTwelve := PREFIX + firstNine

	checkDigit, err := getIsbn13CheckDigit(firstTwelve)
	if err != nil {
		return err
	}

	o.isbn13 = firstTwelve + checkDigit

	return nil
}

// Unmartial JSON data from the Open Library dump into an *OpenLibraryEdition.
func (o *OpenLibraryEdition) unmartialJSON(jsonData []byte) error {
	var innerErr error
	jsonparser.EachKey(jsonData, func(i int, v []byte, vt jsonparser.ValueType, err error) {
		if err != nil {
			return
		}

		if vt == jsonparser.Null || vt == jsonparser.NotExist {
			innerErr = err
			return
		}

		switch i {
		case 0: // key
			key, err := jsonparser.ParseString(v)
			if err != nil {
				innerErr = err
				return
			}
			o.olid = getOlidFromKey(key)

		case 1: // ocaid
			o.ocaid, err = jsonparser.ParseString(v)
			if err != nil {
				innerErr = err
				return
			}

		case 2: // isbn_10
			o.isbn10, err = getFirstIsbnFromArray(v)
			if err != nil {
				innerErr = err
				return
			}

		case 3: // isbn_13
			o.isbn13, err = getFirstIsbnFromArray(v)
			if err != nil {
				innerErr = err
				return
			}
		}
	}, paths...)

	// If there's an ISBN 13 and no ISBN 13, try to convert 10 to 13.
	if o.isbn13 == "" && o.isbn10 != "" {
		if err := o.toIsbn13(); err != nil {
			return err
		}
	}

	return innerErr
}

// getOlidFromKey() takes /books/OL1234M and returns OL1234M.
func getOlidFromKey(key string) string {
	v := strings.Split(key, "/")
	return v[(len(v) - 1)]
}

// parseOLLine() reads a line from the Open Library dump, parses it, and
// returns an *OpenLibraryEdition with edition data.
func parseOLLine(line []byte) (*OpenLibraryEdition, error) {
	columns := bytes.Split(line, []byte("\t"))
	if len(columns) != 5 {
		return nil, fmt.Errorf("%v, %w", string(columns[0]), ErrorWrongColCount)
	}

	// bytes == "/type/edition". Is assigning this here causing excess memory allocation? Is it faster defined elsewhere?
	editionType := []byte{47, 116, 121, 112, 101, 47, 101, 100, 105, 116, 105, 111, 110}
	if res := bytes.Compare(columns[0], editionType); res != 0 {
		return nil, ErrorNotEdition
	}

	// jsonData := columns[4]

	o := OpenLibraryEdition{}
	if err := o.unmartialJSON(columns[4]); err != nil {
		return nil, err
	}

	return &o, nil
}

// getFirstIsbnFromArray() reads a []byte of ISBNs in the form ["12345", "67890"]
// and returns the first one.
func getFirstIsbnFromArray(isbns []byte) (string, error) {
	var parsedIsbns []string
	var innerErr error

	// This is a wastful implementation. Could just read to the first comma and strip quotes.
	jsonparser.ArrayEach(isbns, func(element []byte, _ jsonparser.ValueType, _ int, err error) {
		if err != nil {
			innerErr = err
		}

		parsedIsbns = append(parsedIsbns, string(element))
	})

	// Test for length only once the array values are parsed from the bytes; otherwise empty errays are literally
	// the byte values of "[" and "]", and have lengths of 2.
	if len(parsedIsbns) <= 0 {
		return "", nil // Not interested in logging editions with isbn_10 = [], etc.
	}

	return parsedIsbns[0], innerErr
}

func addEditionsToDB(edition *OpenLibraryEdition, stmt *sql.Stmt) error {
	_, err := stmt.Exec(&edition.olid, &edition.ocaid, &edition.isbn13)
	if err != nil {
		return err
	}

	return nil
}
