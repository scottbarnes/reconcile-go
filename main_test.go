package main

import (
	"io"
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
