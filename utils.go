package main

import (
	"database/sql"
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

// getDB gets a SQLite DB based on the name, such as ":MEMORY:".
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

	// Initalize the DB if necessary.
	if _, err := db.Exec(OLSCHEMA); err != nil {
		return nil, err
	}
	return db, nil
}
