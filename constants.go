package main

// var CHUNKSIZE = int64(1000 * 1000 * 1000)
var CHUNKSIZE = int64(10 * 1000 * 1000)

// Set some SQLite options, per https://avi.im/blag/2021/fast-sqlite-inserts/
// sqlite3 options at https://github.com/mattn/go-sqlite3#connection-string
const DBNAME string = "reconcile-go.db?_sync=0&_journal=WAL"
