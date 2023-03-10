package main

import (
	"reflect"
	"testing"
)

// TestCheckDigit13 verifies a few check digits for ISBN 13.
func TestCheckDigit13(t *testing.T) {
	tests := []struct {
		isbn string
		exp  string
	}{
		{isbn: "978819010750", exp: "1"},
		{isbn: "9781590368923", exp: "3"},
		{isbn: "9781590368930", exp: "0"},
	}

	for _, tc := range tests {
		res, err := getIsbn13CheckDigit(tc.isbn)
		if err != nil {
			t.Fatal(err)
		}

		if res != tc.exp {
			t.Fatalf("Expected %s, but got %s", tc.exp, res)
		}
	}
}

// TestGetChunks parses a test file for comparison against predetermined
// outputs.
func TestGetChunks(t *testing.T) {
	tests := []struct {
		name      string
		chunkSize int64
		expChunks []*Chunk
	}{
		{
			name:      "ThreeChunks",
			chunkSize: int64(2200), expChunks: []*Chunk{
				{filename: "./testdata/chunkTestData.txt", start: 0, end: 2475},
				{filename: "./testdata/chunkTestData.txt", start: 2476, end: 4924},
				{filename: "./testdata/chunkTestData.txt", start: 4925, end: 6968},
			},
		}, {
			name:      "OneBigChunk",
			chunkSize: int64(1000 * 1000), expChunks: []*Chunk{
				{filename: "./testdata/chunkTestData.txt", start: 0, end: 6968},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resChunks, err := getChunks(tc.chunkSize, "./testdata/chunkTestData.txt")
			if err != nil {
				t.Fatal(err)
			}

			for i, resChunk := range resChunks {
				if !reflect.DeepEqual(resChunk, tc.expChunks[i]) {
					t.Fatalf("Expected %v, but got %v", tc.expChunks[i], resChunk)
				}
			}
		})
	}
}
