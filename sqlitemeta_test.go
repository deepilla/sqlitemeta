package sqlitemeta_test

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	// The sqlite driver doesn't support URI-format database names.
	// Running the tests with an in-memory database creates a file
	// named "file::memory:?cache=shared" in the root directory.
	// So default to the more recent go-sqlite3 driver.
	// _ "rsc.io/sqlite"

	_ "github.com/mattn/go-sqlite3"

	meta "github.com/deepilla/sqlitemeta"
)

type dbMode uint

const (
	dbModeUnknown dbMode = iota
	dbModeMemory
	dbModeFile
	dbModeTemp
)

func (m *dbMode) Set(s string) error {

	switch strings.ToLower(s) {
	case "memory":
		*m = dbModeMemory
	case "file":
		*m = dbModeFile
	case "temp":
		*m = dbModeTemp
	default:
		return fmt.Errorf("must be 'memory', 'file' or 'temp'")
	}

	return nil
}

func (m dbMode) String() string {

	switch m {
	case dbModeMemory:
		return "memory-based"
	case dbModeFile:
		return "file-based"
	case dbModeTemp:
		return "temp"
	default:
		return "Unknown"
	}
}

var newDB func() (*sql.DB, func(), error)

func TestMain(m *testing.M) {

	dbmode := dbModeMemory // default to in-memory database

	flag.Var(&dbmode, "sqlitemeta.db", "run tests on a memory-based, file-based or temp database")
	flag.Parse()

	switch dbmode {
	case dbModeMemory:
		newDB = memoryDB
	case dbModeFile:
		newDB = fileDB
	case dbModeTemp:
		newDB = tempDB
	default:
		fmt.Printf("Unknown database mode %v", dbmode)
		os.Exit(2)
	}

	os.Exit(m.Run())
}

func TestSchemaNames(t *testing.T) {
	testWithDB(t, testSchemaNames)
}

func testSchemaNames(t *testing.T, db *sql.DB) {

	data := []struct {
		Title   string
		SQL     []string
		Schemas []string
	}{
		{
			Title: "New Database",
			Schemas: []string{
				"main",
			},
		},
		{
			Title: "Add Temp Table",
			SQL: []string{
				"CREATE TEMP TABLE test(x)",
			},
			Schemas: []string{
				"main",
				"temp",
			},
		},
		{
			Title: "Attach Database",
			SQL: []string{
				"ATTACH DATABASE ':memory:' AS aux",
			},
			Schemas: []string{
				"aux",
				"main",
				"temp",
			},
		},
	}

	for _, test := range data {

		exec(t, db, test.SQL)

		got, err := meta.SchemaNames(db)
		if err != nil {
			t.Fatalf("%s: SchemaNames returned error %s", test.Title, err)
		}

		if !equalStringSlices(test.Schemas, got) {
			t.Errorf("%s: Expected schemas %v, got %v", test.Title, test.Schemas, got)
		}
	}
}

func TestNames(t *testing.T) {
	testWithDB(t, testNames)
}

func testNames(t *testing.T, db *sql.DB) {

	sqls := []string{
		"DROP TABLE IF EXISTS a",
		"DROP TABLE IF EXISTS b",
		"DROP TABLE IF EXISTS c",

		"DROP VIEW IF EXISTS view_a",
		"DROP VIEW IF EXISTS view_b",

		"CREATE TABLE a(x)",
		"CREATE INDEX idx_a ON a(x)",
		"CREATE VIEW view_a AS SELECT * FROM a",

		"CREATE TABLE b(x)",
		"CREATE VIEW view_b AS SELECT * FROM b",
		`CREATE TRIGGER trigger_b AFTER INSERT ON b
            BEGIN
                DELETE FROM a WHERE x = NEW.x;
            END`,

		"CREATE TABLE c(x)",
	}

	data := []struct {
		Title string
		Funcs []func(db *sql.DB) ([]string, error)
		Names []string
	}{
		{
			Title: "Main Tables",
			Funcs: []func(db *sql.DB) ([]string, error){
				meta.TableNames,
				meta.Main.TableNames,
			},
			Names: []string{
				"a",
				"b",
				"c",
			},
		},
		{
			Title: "Main Views",
			Funcs: []func(db *sql.DB) ([]string, error){
				meta.ViewNames,
				meta.Main.ViewNames,
			},
			Names: []string{
				"view_a",
				"view_b",
			},
		},
		{
			Title: "Main Triggers",
			Funcs: []func(db *sql.DB) ([]string, error){
				meta.TriggerNames,
				meta.Main.TriggerNames,
			},
			Names: []string{
				"trigger_b",
			},
		},
		{
			Title: "Main Indexes",
			Funcs: []func(db *sql.DB) ([]string, error){
				meta.IndexNames,
				meta.Main.IndexNames,
			},
			Names: []string{
				"idx_a",
			},
		},
		{
			Title: "Temp Names",
			Funcs: []func(db *sql.DB) ([]string, error){
				meta.Temp.TableNames,
				meta.Temp.ViewNames,
				meta.Temp.TriggerNames,
				meta.Temp.IndexNames,
			},
		},
	}

	exec(t, db, sqls)

	for _, test := range data {
		for i, fn := range test.Funcs {

			got, err := fn(db)
			if err != nil {
				t.Fatalf("%s (%d): Names method returned %s", test.Title, i+1, err)
			}

			if !equalStringSlices(got, test.Names) {
				t.Errorf("%s (%d): Expected names %v, got %v", test.Title, i+1, test.Names, got)
			}
		}
	}
}

func TestColumns(t *testing.T) {
	testWithDB(t, testColumns)
}

func testColumns(t *testing.T, db *sql.DB) {

	data := []struct {
		Title     string
		TableName string
		SQL       []string
		Columns   []meta.Column
	}{
		{
			Title:     "No Table",
			TableName: "xxxxxx",
		},
		{
			Title:     "Basic",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
                    x TEXT,
                    y REAL NOT NULL,
                    z DATETIME NOT NULL DEFAULT 'now'
                )`,
			},
			Columns: []meta.Column{
				{
					ID:   0,
					Name: "x",
					Type: "TEXT",
				},
				{
					ID:      1,
					Name:    "y",
					Type:    "REAL",
					NotNull: true,
				},
				{
					ID:      2,
					Name:    "z",
					Type:    "DATETIME",
					NotNull: true,
					Default: []byte("'now'"),
				},
			},
		},
		{
			Title:     "Integer Primary Key",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
                    x INTEGER PRIMARY KEY
                )`,
			},
			Columns: []meta.Column{
				{
					ID:         0,
					Name:       "x",
					Type:       "INTEGER",
					PrimaryKey: 1,
				},
			},
		},
		{
			Title:     "Without Rowid",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
                    x,
                    y,
                    z,
                    PRIMARY KEY (z, y)
                ) WITHOUT ROWID`,
			},
			Columns: []meta.Column{
				{
					ID:   0,
					Name: "x",
				},
				{
					ID:   1,
					Name: "y",
					// SQLite forces Primary Key columns in WITHOUT ROWID
					// tables to be NOTNULL.
					NotNull:    true,
					PrimaryKey: 2,
				},
				{
					ID:   2,
					Name: "z",
					// SQLite forces Primary Key columns in WITHOUT ROWID
					// tables to be NOTNULL.
					NotNull:    true,
					PrimaryKey: 1,
				},
			},
		},
		{
			Title:     "View 1",
			TableName: "v",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`DROP VIEW IF EXISTS v`,
				`CREATE TABLE a (
                    x BOOLEAN,
                    y DATETIME NOT NULL,
                    z DATETIME NOT NULL DEFAULT 'now'
                )`,
				"CREATE VIEW v AS SELECT y, x FROM a",
			},
			Columns: []meta.Column{
				{
					ID:   0,
					Name: "y",
					Type: "DATETIME",
				},
				{
					ID:   1,
					Name: "x",
					Type: "BOOLEAN",
				},
			},
		},
		{
			Title:     "View 2",
			TableName: "v",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`DROP VIEW IF EXISTS v`,
				`CREATE TABLE a (
                    x BOOLEAN,
                    y DATETIME NOT NULL,
                    z TEXT NOT NULL DEFAULT 'hello'
                )`,
				"CREATE VIEW v AS SELECT ROWID, z FROM a",
			},
			Columns: []meta.Column{
				{
					ID:   0,
					Name: "rowid",
					Type: "INTEGER",
				},
				{
					ID:   1,
					Name: "z",
					Type: "TEXT",
				},
			},
		},
		{
			Title:     "View 3",
			TableName: "v",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`DROP VIEW IF EXISTS v`,
				`CREATE TABLE a (
                    x INTEGER,
                    y REAL NOT NULL,
                    z VARCHAR(255) NOT NULL DEFAULT 'hello'
                )`,
				"CREATE VIEW v(a, b) AS SELECT y, z FROM a",
			},
			Columns: []meta.Column{
				{
					ID:   0,
					Name: "a",
					Type: "REAL",
				},
				{
					ID:   1,
					Name: "b",
					Type: "VARCHAR(255)",
				},
			},
		},
	}

	funcs := []func(*sql.DB, string) ([]meta.Column, error){
		meta.Columns,
		meta.Main.Columns,
	}

	for _, test := range data {

		exec(t, db, test.SQL)
		for i, f := range funcs {

			prefix := fmt.Sprintf("%s (%d)", test.Title, i+1)

			got, err := f(db, test.TableName)
			if err != nil {
				t.Fatalf("%s: Columns(%q) returned error %s", prefix, test.TableName, err)
			}

			compareStructSlices(t, prefix, "column", "column(s)", test.Columns, got)
		}
	}
}

func TestForeignKeys(t *testing.T) {
	testWithDB(t, testForeignKeys)
}

func testForeignKeys(t *testing.T, db *sql.DB) {

	data := []struct {
		Title       string
		TableName   string
		SQL         []string
		ForeignKeys []meta.ForeignKey
	}{
		{
			Title:     "No Table",
			TableName: "xxxxxx",
		},
		{
			Title:     "No Foreign Keys",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
			},
		},
		{
			Title:     "Shorthand 1",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x REFERENCES parent
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey:   make([]sql.NullString, 1),
				},
			},
		},
		{
			Title:     "Shorthand 2",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x REFERENCES parent(px)
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
		{
			Title:     "Longhand",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
		{
			Title:     "Longhand, Multi-Column 1",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					FOREIGN KEY(x,y) REFERENCES parent
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"x", "y"},
					ParentTable: "parent",
					ParentKey:   make([]sql.NullString, 2),
				},
			},
		},
		{
			Title:     "Longhand, Multi-Column 2",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					FOREIGN KEY(x,y) REFERENCES parent(px,py)
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"x", "y"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
						nullString("py"),
					},
				},
			},
		},
		{
			// Potentially fragile test:
			// Is the order of multiple foreign keys stable?
			Title:     "Multiple Foreign Keys",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					FOREIGN KEY(x) REFERENCES parent1(px),
					FOREIGN KEY(y) REFERENCES parent2(py)
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"y"},
					ParentTable: "parent2",
					ParentKey: []sql.NullString{
						nullString("py"),
					},
				},
				{
					ID:          1,
					ChildKey:    []string{"x"},
					ParentTable: "parent1",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
		{
			Title:     "Set NULL, Set Default",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
						ON DELETE SET NULL
						ON UPDATE SET DEFAULT
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
					OnDelete: meta.ForeignKeyActionSetNull,
					OnUpdate: meta.ForeignKeyActionSetDefault,
				},
			},
		},
		{
			Title:     "Cascade, Restrict",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
						ON DELETE CASCADE
						ON UPDATE RESTRICT
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
					OnDelete: meta.ForeignKeyActionCascade,
					OnUpdate: meta.ForeignKeyActionRestrict,
				},
			},
		},
		{
			Title:     "No Action",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
						ON DELETE NO ACTION
				)`,
			},
			ForeignKeys: []meta.ForeignKey{
				{
					ID:          0,
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
	}

	funcs := []func(*sql.DB, string) ([]meta.ForeignKey, error){
		meta.ForeignKeys,
		meta.Main.ForeignKeys,
	}

	for _, test := range data {
		for i, f := range funcs {

			exec(t, db, test.SQL)
			prefix := fmt.Sprintf("%s (%d)", test.Title, i+1)

			got, err := f(db, test.TableName)
			if err != nil {
				t.Fatalf("%s: ForeignKeys(%q) returned error %s", prefix, test.TableName, err)
			}

			compareStructSlices(t, test.Title, "foreign key", "foreign key(s)", test.ForeignKeys, got)
		}
	}
}

func TestIndexes(t *testing.T) {
	testWithDB(t, testIndexes)
}

func testIndexes(t *testing.T, db *sql.DB) {

	data := []struct {
		Title     string
		TableName string
		SQL       []string
		Indexes   []meta.Index
	}{
		{
			Title:     "No Table",
			TableName: "xxxxxxx",
		},
		{
			Title:     "No Index 1",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
			},
		},
		{
			Title:     "No Index 2",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x INTEGER PRIMARY KEY
				)`,
			},
		},
		{
			Title:     "Basic",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
				`CREATE INDEX idx1 ON a(x)`,
			},
			Indexes: []meta.Index{
				{
					Name: "idx1",
					Type: meta.IndexTypeUser,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Title:     "Unique",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
				`CREATE UNIQUE INDEX idx1 ON a(x)`,
			},
			Indexes: []meta.Index{
				{
					Name:     "idx1",
					Type:     meta.IndexTypeUser,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Title:     "Partial",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
				`CREATE INDEX idx1 ON a(x) WHERE x IS NOT NULL`,
			},
			Indexes: []meta.Index{
				{
					Name:      "idx1",
					Type:      meta.IndexTypeUser,
					IsPartial: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Title:     "Unique & Partial",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
				`CREATE UNIQUE INDEX idx1 ON a(x) WHERE x IS NOT NULL`,
			},
			Indexes: []meta.Index{
				{
					Name:      "idx1",
					Type:      meta.IndexTypeUser,
					IsUnique:  true,
					IsPartial: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Title:     "Unique Constraint",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x UNIQUE
				)`,
			},
			Indexes: []meta.Index{
				{
					Name:     "sqlite_autoindex_a_1",
					Type:     meta.IndexTypeUnique,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Title:     "Primary Key 1",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x PRIMARY KEY
				)`,
			},
			Indexes: []meta.Index{
				{
					Name:     "sqlite_autoindex_a_1",
					Type:     meta.IndexTypePrimaryKey,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Title:     "Primary Key 2",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					PRIMARY KEY(x)
				)`,
			},
			Indexes: []meta.Index{
				{
					Name:     "sqlite_autoindex_a_1",
					Type:     meta.IndexTypePrimaryKey,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Title:     "Multi-Column",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					z
				)`,
				`CREATE UNIQUE INDEX idx1 ON a(x, y, z)`,
			},
			Indexes: []meta.Index{
				{
					Name:     "idx1",
					Type:     meta.IndexTypeUser,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
						nullString("y"),
						nullString("z"),
					},
				},
			},
		},
		{
			// Potentially fragile test.
			// Is the order of multiple indexes stable?
			Title:     "Multiple Indexes",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x PRIMARY KEY,
					y UNIQUE,
					z
				)`,
				`CREATE INDEX idx1 ON a(z) WHERE z IS NOT NULL`,
			},
			Indexes: []meta.Index{
				{
					Name:      "idx1",
					Type:      meta.IndexTypeUser,
					IsPartial: true,
					ColumnNames: []sql.NullString{
						nullString("z"),
					},
				},
				{
					Name:     "sqlite_autoindex_a_2",
					Type:     meta.IndexTypeUnique,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("y"),
					},
				},
				{
					Name:     "sqlite_autoindex_a_1",
					Type:     meta.IndexTypePrimaryKey,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Title:     "Expression",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y
				)`,
				`CREATE INDEX idx1 ON a(x+y)`,
			},
			Indexes: []meta.Index{
				{
					Name:        "idx1",
					Type:        meta.IndexTypeUser,
					ColumnNames: make([]sql.NullString, 1),
				},
			},
		},
		{
			Title:     "Mixed Columns/Expressions",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					w,
					x,
					y,
					z
				)`,
				`CREATE INDEX idx1 ON a(w, x+y, z)`,
			},
			Indexes: []meta.Index{
				{
					Name: "idx1",
					Type: meta.IndexTypeUser,
					ColumnNames: []sql.NullString{
						nullString("w"),
						{},
						nullString("z"),
					},
				},
			},
		},
		{
			Title:     "Multiple Expressions",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					w,
					x,
					y,
					z
				)`,
				`CREATE INDEX idx1 ON a(w*x, y+z)`,
			},
			Indexes: []meta.Index{
				{
					Name:        "idx1",
					Type:        meta.IndexTypeUser,
					ColumnNames: make([]sql.NullString, 2),
				},
			},
		},
	}

	funcs := []func(*sql.DB, string) ([]meta.Index, error){
		meta.Indexes,
		meta.Main.Indexes,
	}

	for _, test := range data {

		exec(t, db, test.SQL)
		for i, f := range funcs {

			prefix := fmt.Sprintf("%s (%d)", test.Title, i+1)

			got, err := f(db, test.TableName)
			if err != nil {
				t.Fatalf("%s: Indexes(%q) returned error %s", prefix, test.TableName, err)
			}

			compareStructSlices(t, prefix, "index", "index(es)", test.Indexes, got)
		}
	}
}

func TestIndexColumns(t *testing.T) {
	testWithDB(t, testIndexColumns)
}

func testIndexColumns(t *testing.T, db *sql.DB) {

	type Columns struct {
		Key []meta.IndexColumn
		Aux []meta.IndexColumn
	}

	data := []struct {
		Title   string
		SQL     []string
		Columns map[string]Columns
	}{
		{
			Title: "No Index",
			Columns: map[string]Columns{
				"xxxxxx": {},
			},
		},
		{
			Title: "One Column",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
                    y COLLATE NOCASE
				)`,
				`CREATE INDEX idx1 ON a(x)`,
				`CREATE INDEX idx2 ON a(y)`,
				`CREATE INDEX idx3 ON a(y COLLATE RTRIM)`,
				`CREATE INDEX idx4 ON a(y COLLATE BINARY DESC)`,
				`CREATE INDEX idx5 ON a(x+y DESC)`,
			},
			Columns: map[string]Columns{
				"idx1": {
					Key: []meta.IndexColumn{
						{
							Name:      nullString("x"),
							Rank:      0,
							TableRank: 0,
							Collation: "BINARY",
							IsKey:     true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      1,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
				"idx2": {
					Key: []meta.IndexColumn{
						{
							Name:      nullString("y"),
							Rank:      0,
							TableRank: 1,
							Collation: "NOCASE",
							IsKey:     true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      1,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
				"idx3": {
					Key: []meta.IndexColumn{
						{
							Name:      nullString("y"),
							Rank:      0,
							TableRank: 1,
							Collation: "RTRIM",
							IsKey:     true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      1,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
				"idx4": {
					Key: []meta.IndexColumn{
						{
							Name:       nullString("y"),
							Rank:       0,
							TableRank:  1,
							Collation:  "BINARY",
							Descending: true,
							IsKey:      true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      1,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
				"idx5": {
					Key: []meta.IndexColumn{
						{
							Rank:       0,
							TableRank:  meta.TableRankExpr,
							Collation:  "BINARY",
							Descending: true,
							IsKey:      true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      1,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
			},
		},
		{
			Title: "Two Columns",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
                    y COLLATE NOCASE
				)`,
				`CREATE INDEX idx1 ON a(x, y)`,
				`CREATE INDEX idx2 ON a(y, x DESC)`,
				`CREATE INDEX idx3 ON a(x COLLATE NOCASE DESC, x+y)`,
				`CREATE INDEX idx4 ON a(2*x, 2*y)`,
			},
			Columns: map[string]Columns{
				"idx1": {
					Key: []meta.IndexColumn{
						{
							Name:      nullString("x"),
							Rank:      0,
							TableRank: 0,
							Collation: "BINARY",
							IsKey:     true,
						},
						{
							Name:      nullString("y"),
							Rank:      1,
							TableRank: 1,
							Collation: "NOCASE",
							IsKey:     true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      2,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
				"idx2": {
					Key: []meta.IndexColumn{
						{
							Name:      nullString("y"),
							Rank:      0,
							TableRank: 1,
							Collation: "NOCASE",
							IsKey:     true,
						},
						{
							Name:       nullString("x"),
							Rank:       1,
							TableRank:  0,
							Collation:  "BINARY",
							Descending: true,
							IsKey:      true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      2,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
				"idx3": {
					Key: []meta.IndexColumn{
						{
							Name:       nullString("x"),
							Rank:       0,
							TableRank:  0,
							Collation:  "NOCASE",
							Descending: true,
							IsKey:      true,
						},
						{
							Rank:      1,
							TableRank: meta.TableRankExpr,
							Collation: "BINARY",
							IsKey:     true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      2,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
				"idx4": {
					Key: []meta.IndexColumn{
						{
							Rank:      0,
							TableRank: meta.TableRankExpr,
							Collation: "BINARY",
							IsKey:     true,
						},
						{
							Rank:      1,
							TableRank: meta.TableRankExpr,
							Collation: "BINARY",
							IsKey:     true,
						},
					},
					Aux: []meta.IndexColumn{
						{
							Rank:      2,
							TableRank: meta.TableRankRowID,
							Collation: "BINARY",
						},
					},
				},
			},
		},
	}

	funcData := []struct {
		Aux  bool
		Name string
		Func func(*sql.DB, string) ([]meta.IndexColumn, error)
	}{
		{
			Name: "IndexColumns",
			Func: meta.IndexColumns,
		},
		{
			Name: "IndexColumns",
			Func: meta.Main.IndexColumns,
		},
		{
			Aux:  true,
			Name: "IndexColumnsAux",
			Func: meta.IndexColumnsAux,
		},
		{
			Aux:  true,
			Name: "IndexColumnsAux",
			Func: meta.Main.IndexColumnsAux,
		},
	}

	for _, test := range data {

		exec(t, db, test.SQL)

		for i, f := range funcData {
			for indexName, columns := range test.Columns {

				prefix := fmt.Sprintf("%s %s (%d)", test.Title, indexName, i+1)

				got, err := f.Func(db, indexName)
				if err != nil {
					t.Fatalf("%s: %s(%q) returned error %s", prefix, f.Name, indexName, err)
				}

				exp := columns.Key
				if f.Aux {
					exp = append(exp, columns.Aux...)
				}

				compareStructSlices(t, prefix, "column", "column(s)", exp, got)
			}
		}
	}
}

func TestNamesBadSchema(t *testing.T) {
	testWithDB(t, testNamesBadSchema)
}

func testNamesBadSchema(t *testing.T, db *sql.DB) {

	data := []struct {
		Title  string
		Func   func(*meta.Schema, *sql.DB) ([]string, error)
		Object string
	}{
		{
			Title:  "TableNames",
			Func:   (*meta.Schema).TableNames,
			Object: "table",
		},
		{
			Title:  "ViewNames",
			Func:   (*meta.Schema).ViewNames,
			Object: "view",
		},
		{
			Title:  "IndexNames",
			Func:   (*meta.Schema).IndexNames,
			Object: "index",
		},
		{
			Title:  "TriggerNames",
			Func:   (*meta.Schema).TriggerNames,
			Object: "trigger",
		},
	}

	schemas := []string{
		"test", // Non-existent database
		"sqlite_master; DROP TABLE users; --", // SQL injection attempt
	}

	for _, schemaName := range schemas {
		for _, test := range data {

			exp := fmt.Errorf("could not get %s names: unknown database '%s'", test.Object, schemaName)
			_, got := test.Func(meta.DB(schemaName), db)

			if !equalErrors(exp, got) {
				t.Errorf("%s.%s: Expected error %v, got %v", schemaName, test.Title, exp, got)
			}
		}
	}
}

func TestBadSchema(t *testing.T) {
	testWithDB(t, testBadSchema)
}

func testBadSchema(t *testing.T, db *sql.DB) {

	data := []struct {
		Title    string
		Func     interface{}
		Object   string
		Property string
	}{
		{
			Title:    "Columns",
			Func:     (*meta.Schema).Columns,
			Object:   "table",
			Property: "columns",
		},
		{
			Title:    "Indexes",
			Func:     (*meta.Schema).Indexes,
			Object:   "table",
			Property: "indexes",
		},
		{
			Title:    "ForeignKeys",
			Func:     (*meta.Schema).ForeignKeys,
			Object:   "table",
			Property: "foreign keys",
		},
		{
			Title:    "IndexColumns",
			Func:     (*meta.Schema).IndexColumns,
			Object:   "index",
			Property: "columns",
		},
		{
			Title:    "IndexColumnsAux",
			Func:     (*meta.Schema).IndexColumnsAux,
			Object:   "index",
			Property: "columns",
		},
	}

	schemas := []string{
		"test", // Non-existent database
		"); DROP TABLE users; --", // SQL injection attempt
	}

	objectName := "test"
	errorf := badSchemaErrorf(t, db)

	for _, schemaName := range schemas {

		schema := meta.DB(schemaName)
		for _, test := range data {

			exp := fmt.Errorf("could not get %s for %s %s: %s", test.Property, test.Object, objectName, errorf(schemaName))

			_, got, err := callSliceErrorFunc(test.Func, schema, db, objectName)
			if err != nil {
				t.Fatalf("%s.%s: %s", schemaName, test.Title, err)
			}

			if !equalErrors(exp, got) {
				t.Errorf("%s.%s: Expected error %v, got %v", schemaName, test.Title, exp, got)
			}
		}
	}
}

func TestBadTarget(t *testing.T) {
	testWithDB(t, testBadTarget)
}

func testBadTarget(t *testing.T, db *sql.DB) {

	data := []struct {
		Title string
		Funcs []interface{}
	}{
		{
			Title: "Columns",
			Funcs: []interface{}{
				meta.Columns,
				meta.Main.Columns,
				meta.Temp.Columns,
			},
		},
		{
			Title: "Indexes",
			Funcs: []interface{}{
				meta.Indexes,
				meta.Main.Indexes,
				meta.Temp.Indexes,
			},
		},
		{
			Title: "ForeignKeys",
			Funcs: []interface{}{
				meta.ForeignKeys,
				meta.Main.ForeignKeys,
				meta.Temp.ForeignKeys,
			},
		},
		{
			Title: "IndexColumns",
			Funcs: []interface{}{
				meta.IndexColumns,
				meta.Main.IndexColumns,
				meta.Temp.IndexColumns,
			},
		},
		{
			Title: "IndexColumnsAux",
			Funcs: []interface{}{
				meta.IndexColumnsAux,
				meta.Main.IndexColumnsAux,
				meta.Temp.IndexColumnsAux,
			},
		},
	}

	names := []string{
		"",                        // Empty string
		"xxxxxxxxx",               // Non-existent table
		"); DROP TABLE users; --", // SQL injection attempt
	}

	for _, objectName := range names {
		for _, test := range data {
			for i, f := range test.Funcs {

				n, e, err := callSliceErrorFunc(f, db, objectName)
				if err != nil {
					t.Fatalf("%s (%d): %s", test.Title, i+1, err)
				}

				if e != nil {
					t.Errorf("%s (%d): Expected %s(%q) to return a Nil error, got %s", test.Title, i+1, test.Title, objectName, e)
				}

				if n != 0 {
					t.Errorf("%s (%d): Expected %s(%q) to return zero items, got %d", test.Title, i+1, test.Title, objectName, n)
				}
			}
		}
	}
}

func TestScanIndexType(t *testing.T) {

	invalid := func(v interface{}) error {
		return fmt.Errorf("invalid IndexType: %T %v", v, v)
	}

	unsupported := func(s string) error {
		return fmt.Errorf("unsupported IndexType: %q", s)
	}

	data := []struct {
		Title  string
		Values []interface{}
		Type   meta.IndexType
		Err    error
	}{
		{
			Title: "Default",
			Values: []interface{}{
				"c",
				[]byte("C"),
			},
			Type: meta.IndexTypeUser,
		},
		{
			Title: "Unique",
			Values: []interface{}{
				"u",
				[]byte("U"),
			},
			Type: meta.IndexTypeUnique,
		},
		{
			Title: "Primary Key",
			Values: []interface{}{
				"pk",
				[]byte("PK"),
			},
			Type: meta.IndexTypePrimaryKey,
		},
		{
			Title: "Unexpected String 1",
			Values: []interface{}{
				"",
			},
			Err: unsupported(""),
		},
		{
			Title: "Unexpected String 2",
			Values: []interface{}{
				"unknown",
			},
			Err: unsupported("unknown"),
		},
		{
			Title: "Invalid Type 1",
			Values: []interface{}{
				nil,
			},
			Err: invalid(nil),
		},
		{
			Title: "Invalid Type 2",
			Values: []interface{}{
				42,
			},
			Err: invalid(42),
		},
	}

	for _, test := range data {
		for i, v := range test.Values {

			var typ meta.IndexType
			err := typ.Scan(v)

			if !equalErrors(err, test.Err) {
				t.Errorf("%s (%d): Expected error %v, got %v", test.Title, i+1, test.Err, err)
			}

			if typ != test.Type {
				t.Errorf("%s (%d): Expected IndexType %v, got %v", test.Title, i+1, test.Type, typ)
			}
		}
	}
}

func TestScanForeignKeyAction(t *testing.T) {

	invalid := func(v interface{}) error {
		return fmt.Errorf("invalid ForeignKeyAction: %T %v", v, v)
	}

	unsupported := func(s string) error {
		return fmt.Errorf("unsupported ForeignKeyAction: %q", s)
	}

	data := []struct {
		Title  string
		Values []interface{}
		Action meta.ForeignKeyAction
		Err    error
	}{
		{
			Title: "None",
			Values: []interface{}{
				"NO ACTION",
				[]byte("No Action"),
			},
			Action: meta.ForeignKeyActionNone,
		},
		{
			Title: "Restrict",
			Values: []interface{}{
				"RESTRICT",
				[]byte("Restrict"),
			},
			Action: meta.ForeignKeyActionRestrict,
		},
		{
			Title: "Set Null",
			Values: []interface{}{
				"SET NULL",
				[]byte("Set NULL"),
			},
			Action: meta.ForeignKeyActionSetNull,
		},
		{
			Title: "Set Default",
			Values: []interface{}{
				"SET DEFAULT",
				[]byte("Set Default"),
			},
			Action: meta.ForeignKeyActionSetDefault,
		},
		{
			Title: "Cascade",
			Values: []interface{}{
				"CASCADE",
				[]byte("Cascade"),
			},
			Action: meta.ForeignKeyActionCascade,
		},
		{
			Title: "Unexpected String 1",
			Values: []interface{}{
				"",
			},
			Err: unsupported(""),
		},
		{
			Title: "Unexpected String 2",
			Values: []interface{}{
				"Unknown",
			},
			Err: unsupported("Unknown"),
		},
		{
			Title: "Invalid Type 1",
			Values: []interface{}{
				nil,
			},
			Err: invalid(nil),
		},
		{
			Title: "Invalid Type 2",
			Values: []interface{}{
				42,
			},
			Err: invalid(42),
		},
	}

	for _, test := range data {
		for i, v := range test.Values {

			var action meta.ForeignKeyAction
			err := action.Scan(v)

			if !equalErrors(err, test.Err) {
				t.Errorf("%s (%d): Expected error %v, got %v", test.Title, i+1, test.Err, err)
			}

			if action != test.Action {
				t.Errorf("%s (%d): Expected ForeignKeyAction %v, got %v", test.Title, i+1, test.Action, action)
			}
		}
	}
}

// DB helpers

func testWithDB(t *testing.T, fn func(t *testing.T, db *sql.DB)) {

	db, close, err := newDB()
	if err != nil {
		t.Fatalf("Could not open db: %s", err)
	}
	defer close()
	fn(t, db)
}

func fileDB() (*sql.DB, func(), error) {

	f, err := ioutil.TempFile("", "sqlitemeta-test")
	if err != nil {
		return nil, nil, err
	}

	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return nil, nil, err
	}

	db, err := openDB(f.Name())
	if err != nil {
		os.Remove(f.Name())
		return nil, nil, err
	}

	return db, func() {
		db.Close()
		os.Remove(f.Name())
	}, nil
}

func memoryDB() (*sql.DB, func(), error) {

	db, err := openDBParams(":memory:", map[string]string{
		"cache": "shared",
	})
	if err != nil {
		return nil, nil, err
	}

	return db, func() {
		db.Close()
	}, nil
}

func tempDB() (*sql.DB, func(), error) {

	db, err := openDB("")
	if err != nil {
		return nil, nil, err
	}

	return db, func() {
		db.Close()
	}, nil
}

func openDBParams(filename string, params map[string]string) (*sql.DB, error) {

	values := url.Values{}
	for k, v := range params {
		values.Set(k, v)
	}

	if s := values.Encode(); s != "" {
		filename += "?" + s
	}

	return openDB("file:" + filename)
}

func openDB(filename string) (*sql.DB, error) {
	return sql.Open("sqlite3", filename)
}

// Misc helpers

func badSchemaErrorf(t *testing.T, db *sql.DB) func(string) string {

	badSchema := "xxxxx"

	_, err := db.Exec("SELECT * FROM pragma_table_info('test', ?)", badSchema)
	if err == nil {
		t.Fatalf("badSchemaErrorf: Query didn't return an error")
	}

	msg := err.Error()

	if !strings.Contains(msg, "unknown database") {
		t.Fatalf("badSchemaErrorf: Expected unknown database error, got %q", msg)
	}

	if !strings.Contains(msg, badSchema) {
		t.Fatalf("badSchemaErrorf: Expected error to contain schema name %q, got %q", badSchema, msg)
	}

	format := strings.Replace(err.Error(), badSchema, "%s", 1)

	return func(s string) string {
		return fmt.Sprintf(format, s)
	}
}

func callSliceErrorFunc(fn interface{}, args ...interface{}) (int, error, error) {

	vArgs := make([]reflect.Value, len(args))
	for i := range args {
		vArgs[i] = reflect.ValueOf(args[i])
	}

	results := reflect.ValueOf(fn).Call(vArgs)
	if len(results) != 2 {
		return 0, nil, fmt.Errorf("Expected function to return 2 values, got %d", len(results))
	}

	n := results[0].Len()
	v := results[len(results)-1].Interface()

	switch v := v.(type) {
	case nil:
		return n, nil, nil
	case error:
		return n, v, nil
	default:
		return 0, nil, fmt.Errorf("Expected function to return an error, got %T %v", v, v)
	}
}

func compareStructSlices(t *testing.T, prefix, thing, things string, exp, got interface{}) {

	vexp := reflect.ValueOf(exp)
	vgot := reflect.ValueOf(got)

	if vgot.Type() != vexp.Type() {
		t.Fatalf("%s: Cannot compare different types %v and %v", prefix, vexp.Type(), vgot.Type())
	}

	if vgot.Len() != vexp.Len() {
		t.Errorf("%s: Expected %d %s, got %d", prefix, vexp.Len(), things, vgot.Len())
		return
	}

	numfields := vgot.Type().Elem().NumField()

	for i := 0; i < vgot.Len(); i++ {
		for j := 0; j < numfields; j++ {

			g := vgot.Index(i).Field(j).Interface()
			e := vexp.Index(i).Field(j).Interface()

			if !reflect.DeepEqual(g, e) {
				t.Errorf("%s: Expected %s %d to have %s %v, got %v", prefix, thing, i+1, vgot.Index(i).Type().Field(j).Name, stringify(e), stringify(g))
			}
		}
	}
}

func equalStringSlices(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

func equalErrors(e1, e2 error) bool {
	switch {
	case e1 == e2:
		return true
	case e1 == nil, e2 == nil:
		return false
	default:
		return e1.Error() == e2.Error()
	}
}

func stringify(v interface{}) string {
	switch v := v.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func nullString(s string) sql.NullString {
	return sql.NullString{
		Valid:  true,
		String: s,
	}
}

func exec(t *testing.T, db *sql.DB, sql []string) {
	for _, q := range sql {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("db.Exec %q returned error %q", q, err)
		}
	}
}
