package main

import (
	"testing"
)

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
