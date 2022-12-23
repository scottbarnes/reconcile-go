package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"testing"
)

// TestToIsbn13 calls the method and verifies the result.
func TestToIsbn13(t *testing.T) {
	book1 := &OpenLibraryEdition{"OL123M", "IA123", "819010750X", ""}
	book1.toIsbn13()
	expected := "9788190107501"

	if book1.isbn13 != expected {
		t.Fatalf("Expected %s, but got %s", expected, book1.isbn13)
	}
}

func BenchmarkRun(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if err := run("./testdata/30kTestEditions.txt", io.Discard); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkRunSeq(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// if err := runSeq("/home/scott/code/reconcile/files/ol_dump_latest.txt", io.Discard); err != nil {
		if err := runSeq("./testdata/50kTestEditions.txt", io.Discard); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkPureRead(b *testing.B) {
	f, err := os.Open("./testdata/50kTestEditions.txt")
	// f, err := os.Open("/home/scott/code/reconcile/files/ol_dump_latest.txt")
	if err != nil {
		b.Fatal(err)
	}

	const maxCapacity = 100 * 100 * 1000 // This size gets through the "ALL" dump.
	buf := make([]byte, maxCapacity)
	sc := bufio.NewScanner(f)
	sc.Buffer(buf, 1)

	b.ResetTimer()
	var lines int
	for sc.Scan() {
		lines++
	}
	fmt.Println("Lines: ", lines)
}
