/*
Package sqlitemeta provides access to SQLite metadata, such as table,
column and index information.

To use this package, establish an SQLite connection (via the database/sql
package) and pass the resulting database handle to the sqlitemeta functions,
e.g.

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

Multiple Databases

In SQLite, a single database connection can access multiple databases,
each with their own schemas and data. In this package, an individual
database is represented by the Schema type.

Most of the top-level functions in this package have matching methods
on the Schema type. Use the Schema methods to restrict scope to a
particular database. Use the top-level functions to operate on the
main database or, for functions that take a table or index name, to
search across all available databases.

When SQLite searches its databases for a particular table or index,
the temp database takes precedence, followed by the main database,
followed by any other attached databases (in order of attachment).

See https://sqlite.org/lang_naming.html for details.
*/
package sqlitemeta
