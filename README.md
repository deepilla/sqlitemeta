# sqlitemeta

[![GoDoc](https://godoc.org/github.com/deepilla/sqlitemeta?status.svg)](https://godoc.org/github.com/deepilla/sqlitemeta)
[![Build Status](https://travis-ci.org/deepilla/sqlitemeta.svg?branch=master)](https://travis-ci.org/deepilla/sqlitemeta)
[![Go Report Card](https://goreportcard.com/badge/github.com/deepilla/sqlitemeta)](https://goreportcard.com/report/github.com/deepilla/sqlitemeta)

SQLitemeta is a Go library that provides access to SQLite metadata such as table, column and index information.

## Installation

    go get github.com/deepilla/sqlitemeta

## Usage

Import the [database/sql](https://golang.org/pkg/database/sql/) package along with an SQLite [database driver](https://github.com/golang/go/wiki/SQLDrivers).

    import "database/sql"
    import _ "github.com/mattn/go-sqlite3"

Import the sqlitemeta package.

    import "github.com/deepilla/sqlitemeta"

Open a database connection and pass the handle to the sqlitemeta functions.

```go
db, err := sql.Open("sqlite3", "/path/to/sqlite.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

tables, err := sqlitemeta.TableNames(db)
if err != nil {
    log.Fatal(err)
}

for _, tbl := range tables {

    indexes, err := sqlitemeta.Indexes(db, tbl)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Table", tbl, "has the following indexes")
    for _, in := range indexes {
        fmt.Println(" -", in.Name)
    }
    fmt.Println()
}
```

## Licensing

sqlitemeta is provided under an [MIT License](http://choosealicense.com/licenses/mit/). See the [LICENSE](LICENSE) file for details.
