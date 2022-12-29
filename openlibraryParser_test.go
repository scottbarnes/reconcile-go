package main

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3" // See http://go-database-sql.org/importing.html for an explanation of this side effect.
)

var expEditions = []*OpenLibraryEdition{
	{"OL001M", "IA001", "", "9788955565683"},
	{"OL002M", "IA002", "0135043948", "9780135043943"},
	{"OL16775850M", "seals0000bekk", "", "9781590368930"},
}

func TestParseOLLine(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		expEdition *OpenLibraryEdition
		expErr     error
	}{
		{
			name: "ISBN13", input: `/type/edition	/books/OL001M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL001M", "isbn_13": ["9788955565683"], "ocaid": "IA001"}`,
			expEdition: &OpenLibraryEdition{olid: "OL001M", ocaid: "IA001", isbn10: "", isbn13: "9788955565683"}, expErr: nil,
		},
		{
			name: "ISBN10", input: `/type/edition	/books/OL002M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL002M", "isbn_10": ["0141439513"], "ocaid": "IA002"}`,
			expEdition: &OpenLibraryEdition{olid: "OL002M", ocaid: "IA002", isbn10: "0141439513", isbn13: "9780141439518"}, expErr: nil,
		},
		// This invalid ISBN 10 produces an invalid ISBN 13. That does not currently matter for our comparison purposes.
		{
			name: "BadISBN10", input: `/type/edition	/books/OL003M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL003M", "isbn_10": ["222222222X"], "ocaid": "IA003"}`,
			expEdition: &OpenLibraryEdition{olid: "OL003M", ocaid: "IA003", isbn10: "222222222X", isbn13: "9782222222224"}, expErr: nil,
		},
		{
			name: "BadISBN13", input: `/type/edition	/books/OL004M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL004M", "isbn_13": ["1234567890123"], "ocaid": "IA004"}`,
			expEdition: &OpenLibraryEdition{olid: "OL004M", ocaid: "IA004", isbn10: "", isbn13: "1234567890123"}, expErr: nil,
		},
		{
			name: "EmptyOCAID", input: `/type/edition	/books/OL005M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL005M", "isbn_13": ["1234567890123"], "ocaid": ""}`,
			expEdition: &OpenLibraryEdition{olid: "OL005M", ocaid: "", isbn10: "", isbn13: "1234567890123"}, expErr: nil,
		},
		{
			name: "NoOCAID", input: `/type/edition	/books/OL006M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL006M", "isbn_13": ["1234567890123"]}`,
			expEdition: &OpenLibraryEdition{olid: "OL006M", ocaid: "", isbn10: "", isbn13: "1234567890123"}, expErr: nil,
		},
		{
			name: "NoISBN", input: `/type/edition	/books/OL007M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL007M", "ocaid": "IA007"}`,
			expEdition: &OpenLibraryEdition{olid: "OL007M", ocaid: "IA007", isbn10: "", isbn13: ""}, expErr: nil,
		},
		{
			name: "TooManyColumns", input: `ExtraCol	/type/edition	/books/OL008M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL008M", "isbn_13": ["9788955565683"], "ocaid": "IA008"}`,
			expEdition: nil, expErr: ErrorWrongColCount,
		},
		{
			name: "OneColumnShort", input: `/books/OL009M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL009M", "isbn_13": ["9788955565683"], "ocaid": "IA009"}`,
			expEdition: nil, expErr: ErrorWrongColCount,
		},
		{
			name: "TwoISBNsofSameType", input: `/type/edition	/books/OL010M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL010M", "isbn_13": ["1234567890123", "9788955565683"], "ocaid": "IA010"}`,
			expEdition: &OpenLibraryEdition{olid: "OL010M", ocaid: "IA010", isbn10: "", isbn13: "1234567890123"}, expErr: nil,
		},
		// Use ISBN 13 when it exists, and don't calculate the ISBN 13 based off the ISBN 10 -- even when the ISBN 10 would generate a different ISBN 13.
		{
			name: "IncompatibleISBN13andISBN10", input: `/type/edition	/books/OL011M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL011M", "isbn_10": ["0135043948"] "isbn_13": ["9788955565683"], "ocaid": "IA011"}`,
			expEdition: &OpenLibraryEdition{olid: "OL011M", ocaid: "IA011", isbn10: "0135043948", isbn13: "9788955565683"}, expErr: nil,
		},
		{
			name: "SkipNonEditions", input: `/type/author	/books/OL001A	6	2020-12-22T19:20:44.396666	{"key": "/authors/OL011A"}`,
			expEdition: nil, expErr: ErrorNotEdition,
		},
		{
			name: "ISBN10WithNon9CharBecomes0000000000", input: `/type/edition	/books/OL012M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL012M", "isbn_10": ["123"], "ocaid": "IA012"}`,
			expEdition: &OpenLibraryEdition{olid: "OL012M", ocaid: "IA012", isbn10: "0000000000", isbn13: "9780000000002"}, expErr: nil,
		},
		{
			name: "ISBN10WithNoValue", input: `/type/edition	/books/OL013M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL013M", "isbn_10": [], "ocaid": "IA013"}`,
			expEdition: &OpenLibraryEdition{olid: "OL013M", ocaid: "IA013"}, expErr: nil,
		},
		{
			name: "ISBN13WithNoValue", input: `/type/edition	/books/OL014M	6	2020-12-22T19:20:44.396666	{"key": "/books/OL014M", "isbn_13": [], "ocaid": "IA014"}`,
			expEdition: &OpenLibraryEdition{olid: "OL014M", ocaid: "IA014"}, expErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			edition, err := parseOLLine([]byte(tc.input))
			if tc.expErr != nil {
				if err == nil {
					t.Fatalf("expected error, but found no error")
				}

				if !errors.Is(err, tc.expErr) {
					t.Fatalf("expected %q, but got %q instead", tc.expErr, err)
				}
			}

			if !reflect.DeepEqual(tc.expEdition, edition) {
				t.Fatalf("expected: %v, but got %v", tc.expEdition, edition)
			}
		})
	}
}

func TestGetOlidFromKey(t *testing.T) {
	key := "/books/OL123M"
	exp := "OL123M"
	res := getOlidFromKey(key)
	if res != exp {
		t.Fatalf("expected %s, but got %s", exp, res)
	}
}

func TestReadFile(t *testing.T) {
	var resEditions []*OpenLibraryEdition

	errCh := make(chan error)
	editionsCh := make(chan *OpenLibraryEdition)

	f, err := os.Open("./testdata/seed_ol_dump_latest.txt")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		defer close(editionsCh)
		err := readFile(f, editionsCh)
		if err != nil {
			errCh <- err
		}
	}()

	for edition := range editionsCh {
		resEditions = append(resEditions, edition)
	}

	// Check expected vs. results.
	for i, v := range resEditions {
		// Only test the first three items in the test data file.
		if i <= 2 {
			if !reflect.DeepEqual(expEditions[i], v) {
				t.Fatalf("expected %v, but got %v", expEditions[i], v)
			}
		}
	}
}

func TestGetEditions(t *testing.T) {
	var resEditions []*OpenLibraryEdition
	chunkSize := int64(1000)
	editionsCh := make(chan *OpenLibraryEdition)
	out := os.Stdout
	inFile := "./testdata/chunkTestData.txt"
	expEditions := []*OpenLibraryEdition{
		{olid: "OL001M", ocaid: "IA001", isbn10: "", isbn13: "9788955565683"},
		{olid: "OL002M", ocaid: "IA002", isbn10: "0135043948", isbn13: "9780135043943"},
		{olid: "OL16775850M", ocaid: "seals0000bekk", isbn10: "", isbn13: "9781590368930"},
		{olid: "OL10738135M", ocaid: "temporarymarriag00thor", isbn10: "037310491X", isbn13: "9780373104918"},
		{olid: "OL10737124M", ocaid: "", isbn10: "0373086970", isbn13: "9780373086979"},
		{olid: "OL10737484M", ocaid: "onemanslove00lisa", isbn10: "0373093586", isbn13: "9780373093588"},
		{olid: "OL10737723M", ocaid: "", isbn10: "0373096879", isbn13: "9780373096879"},
	}

	// Read in editions
	go func() {
		for edition := range editionsCh {
			resEditions = append(resEditions, edition)
		}
	}()

	if err := getEditions(inFile, out, editionsCh, chunkSize); err != nil {
		fmt.Fprintln(os.Stderr, err)
		t.Fatal(err)
	}

	// for _, v := range resEditions {
	// 	fmt.Printf("%#v\n", v)
	// }
	if !reflect.DeepEqual(expEditions, resEditions) {
		t.Fatalf("Expected %v, but got %v", expEditions, resEditions)
	}
}

func TestAddEditionsToDB(t *testing.T) {
	dbName := ":memory:"
	db, err := getDB(dbName)
	if err != nil {
		t.Fatal(err)
	}

	testEditions := expEditions
	expDBItems := []struct {
		olid   string
		ocaid  string
		isbn13 string
	}{
		{"OL001M", "IA001", "9788955565683"},
		{"OL002M", "IA002", "9780135043943"},
		{"OL16775850M", "seals0000bekk", "9781590368930"},
	}

	stmt, err := db.Prepare("INSERT INTO ol VALUES(NULL, ?, ?, ?)")
	if err != nil {
		t.Fatal(err)
	}

	for _, edition := range testEditions {
		addEditionsToDB(edition, stmt)
	}

	// Query DB to get items added from channel.
	rows, err := db.Query("SELECT * FROM ol")
	if err != nil {
		t.Fatal(err)
	}

	// Throw away variable because rows.Scan() needs to assign all the values.
	count := 0
	for rows.Next() {
		exp := struct {
			olid   string
			ocaid  string
			isbn13 string
		}{}
		id := 0

		err := rows.Scan(&id, &exp.olid, &exp.ocaid, &exp.isbn13)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(exp, expDBItems[count]) {
			t.Fatalf("expected %v, but got %v", exp, expDBItems[count])
		}

		count++
	}
}

// This is broken and does not appear to reflect actual time/op.
// For some reason it gets drastically lower time/op the higher the number of iterations are.
func BenchmarkReadAndParse(b *testing.B) {
	editionsCh := make(chan *OpenLibraryEdition)

	f, err := os.Open("./testdata/50kTestEditions.txt")
	if err != nil {
		b.Fatal(err)
	}

	var count int
	// Just consume editions as fast as possible.
	go func() {
		for {
			count++
			fmt.Println("count is: ", count)
			<-editionsCh
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := readFile(f, editionsCh)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddEditionToDB(b *testing.B) {
	o := &OpenLibraryEdition{olid: "OL123M", ocaid: "IA123", isbn10: "", isbn13: "1111111111111"}
	// const TESTDB string = "reconcile-go.db?_sync=0&_journal=WAL"
	const TESTDB = ":memory:?_sync=0&_journal=WAL"
	db, err := getDB(TESTDB)
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := db.Prepare("INSERT INTO ol VALUES(NULL, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addEditionsToDB(o, stmt)
	}
}

// func BenchmarkUnbufferedChannelEmptyStruct(b *testing.B) {
// 	ch := make(chan struct{})
// 	var count int
// 	go func() {
// 		for {
// 			count++
// 			fmt.Println("count is:", count)
// 			<-ch
// 		}
// 	}()
// 	for i := 0; i < b.N; i++ {
// 		ch <- struct{}{}
// 	}
// }
