package main

import "errors"

var (
	ErrorWrongColCount   = errors.New("invalid number of columns")
	ErrorNotEdition      = errors.New("line is not an edition")
	ErrorNewlineNotFound = errors.New("newline not found")
)
