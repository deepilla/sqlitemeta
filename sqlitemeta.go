package sqlitemeta

import (
	"database/sql"
	"fmt"
	"strings"
)

// A Schema represents a database attached to the current
// database connection.
type Schema struct {
	name string
}

// DB returns a Schema with the given name. It does not verify
// that a database with this name actually exists.
func DB(name string) *Schema {
	return &Schema{name}
}

// Main is the database that was used to open a database
// connection.
var Main = DB("main")

// Temp is the database that holds temporary tables, indexes
// etc.
var Temp = DB("temp")

var noSchema = &Schema{}

// SchemaNames returns the names of the databases attached to
// the given database connection, sorted alphabetically.
func SchemaNames(db *sql.DB) ([]string, error) {

	names, err := queryStrings(db, "SELECT name FROM pragma_database_list ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("could not get schema names: %s", err)
	}

	return names, nil
}

// TableNames returns the names of the tables in the main
// database, sorted alphabetically. Use the Schema.TableNames
// method to query other databases.
func TableNames(db *sql.DB) ([]string, error) {
	return noSchema.TableNames(db)
}

// TableNames returns the names of the tables in this Schema,
// sorted alphabetically.
func (s *Schema) TableNames(db *sql.DB) ([]string, error) {
	return s.masterTableNames(db, "table")
}

// ViewNames returns the names of the views in the main database,
// sorted alphabetically. Use the Schema.ViewNames method to query
// other databases.
func ViewNames(db *sql.DB) ([]string, error) {
	return noSchema.ViewNames(db)
}

// ViewNames returns the names of the views in this Schema, sorted
// alphabetically.
func (s *Schema) ViewNames(db *sql.DB) ([]string, error) {
	return s.masterTableNames(db, "view")
}

// TriggerNames returns the names of the triggers in the main
// database, sorted alphabetically. Use the Schema.TriggerNames
// method to query other databases.
func TriggerNames(db *sql.DB) ([]string, error) {
	return noSchema.TriggerNames(db)
}

// TriggerNames returns the names of the triggers in this Schema,
// sorted alphabetically.
func (s *Schema) TriggerNames(db *sql.DB) ([]string, error) {
	return s.masterTableNames(db, "trigger")
}

// IndexNames returns the names of the indexes in the main
// database, sorted alphabetically. Use the Schema.IndexNames
// method to query other databases.
func IndexNames(db *sql.DB) ([]string, error) {
	return noSchema.IndexNames(db)
}

// IndexNames returns the names of the indexes in this Schema,
// sorted alphabetically.
func (s *Schema) IndexNames(db *sql.DB) ([]string, error) {
	return s.masterTableNames(db, "index")
}

// Column represents a column in a table.
type Column struct {
	ID         int
	Name       string
	Type       string
	NotNull    bool
	Default    []byte
	PrimaryKey int
}

// Columns returns column information for the given table.
//
// If no such table is found in any of the available databases
// (see Multiple Databases above), Columns returns an empty
// slice.
func Columns(db *sql.DB, tableName string) ([]Column, error) {
	return noSchema.Columns(db, tableName)
}

// Columns returns column information for the given table.
//
// If no such table is found in this Schema, Columns returns
// an empty slice.
func (s *Schema) Columns(db *sql.DB, tableName string) ([]Column, error) {

	params := []interface{}{tableName}
	if s.name != "" {
		params = append(params, s.name)
	}

	q :=
		`SELECT
			cid,
			name,
			type,
			"notnull",
			dflt_value,
			pk
		FROM
			pragma_table_info(` + placeholdersFor(params) + `)
		ORDER BY
			cid`

	var columns []Column

	err := queryRows(&columns, db, q, params...)
	if err != nil {
		return nil, fmt.Errorf("could not get columns for table %s: %s", tableName, err)
	}

	return columns, nil
}

// A ForeignKeyAction describes what happens to the child rows
// of a foreign key when the parent key values are updated or
// deleted.
//
// See https://sqlite.org/foreignkeys.html for more on foreign
// key actions.
type ForeignKeyAction uint

const (
	// ForeignKeyActionNone indicates that no special action is
	// taken. This is the default behaviour.
	ForeignKeyActionNone ForeignKeyAction = iota

	// ForeignKeyActionRestrict prohibits the application from
	// updating or deleting parent keys.
	ForeignKeyActionRestrict

	// ForeignKeyActionSetNull sets any child key values to NULL
	// when a parent key value is updated or deleted.
	ForeignKeyActionSetNull

	// ForeignKeyActionSetDefault sets any child key values to
	// their defaults when a parent key value is updated or
	// deleted.
	ForeignKeyActionSetDefault

	// ForeignKeyActionCascade propagates parent key changes to
	// the child rows. When a parent key value is updated, any
	// child key values are similarly updated. When a parent
	// key is deleted, any child rows are also deleted.
	ForeignKeyActionCascade
)

// Scan implements the sql.Scanner interface.
func (v *ForeignKeyAction) Scan(src interface{}) error {

	s, ok := toString(src)
	if !ok {
		return fmt.Errorf("invalid ForeignKeyAction: %T %v", src, src)
	}

	switch strings.ToUpper(s) {
	case "NO ACTION":
		*v = ForeignKeyActionNone
	case "RESTRICT":
		*v = ForeignKeyActionRestrict
	case "SET NULL":
		*v = ForeignKeyActionSetNull
	case "SET DEFAULT":
		*v = ForeignKeyActionSetDefault
	case "CASCADE":
		*v = ForeignKeyActionCascade
	default:
		return fmt.Errorf("unsupported ForeignKeyAction: %q", s)
	}

	return nil
}

// ForeignKey represents a foreign key constraint.
type ForeignKey struct {
	ID          int
	ChildTable  string
	ChildKey    []string
	ParentTable string
	ParentKey   []sql.NullString // Parent key fields are NULL if not specified in the REFERENCES clause.
	OnUpdate    ForeignKeyAction
	OnDelete    ForeignKeyAction
}

// ForeignKeys returns foreign key information for the given
// table.
//
// If no such table is found in any of the available databases
// (see Multiple Databases above), ForeignKeys returns an empty
// slice.
func ForeignKeys(db *sql.DB, tableName string) ([]ForeignKey, error) {
	return noSchema.ForeignKeys(db, tableName)
}

// ForeignKeys returns foreign key information for the given
// table.
//
// If no such table is found in this Schema, ForeignKeys returns
// an empty slice.
func (s *Schema) ForeignKeys(db *sql.DB, tableName string) ([]ForeignKey, error) {

	params := []interface{}{tableName}
	if s.name != "" {
		params = append(params, s.name)
	}

	q :=
		`SELECT
			id,
			"table",
			"from",
			"to",
			on_update,
			on_delete
		FROM
			pragma_foreign_key_list(` + placeholdersFor(params) + `)
		ORDER BY
			id, seq`

	var rows []struct {
		ID       int
		Table    string
		From     string
		To       sql.NullString
		OnUpdate ForeignKeyAction
		OnDelete ForeignKeyAction
	}

	err := queryRows(&rows, db, q, params...)
	if err != nil {
		return nil, fmt.Errorf("could not get foreign keys for table %s: %s", tableName, err)
	}

	var fk *ForeignKey
	var foreignKeys []ForeignKey

	for _, r := range rows {

		if fk == nil || r.ID != fk.ID {

			foreignKeys = append(foreignKeys, ForeignKey{
				ID:          r.ID,
				ChildTable:  tableName,
				ParentTable: r.Table,
				OnUpdate:    r.OnUpdate,
				OnDelete:    r.OnDelete,
			})

			fk = &foreignKeys[len(foreignKeys)-1]
		}

		fk.ChildKey = append(fk.ChildKey, r.From)
		fk.ParentKey = append(fk.ParentKey, r.To)
	}

	return foreignKeys, nil
}

// IndexType indicates how an index was created.
type IndexType uint

const (
	// IndexTypeNormal denotes an index created by the user
	// with a CREATE INDEX statement.
	IndexTypeNormal IndexType = iota

	// IndexTypeUnique denotes an index created by SQLite to
	// enforce a UNIQUE column constraint.
	IndexTypeUnique

	// IndexTypePrimaryKey denotes an index created by SQLite
	// to enforce a PRIMARY KEY clause.
	IndexTypePrimaryKey
)

// Scan implements the sql.Scanner interface.
func (t *IndexType) Scan(src interface{}) error {

	s, ok := toString(src)
	if !ok {
		return fmt.Errorf("invalid IndexType: %T %v", src, src)
	}

	switch strings.ToLower(s) {
	case "c":
		*t = IndexTypeNormal
	case "u":
		*t = IndexTypeUnique
	case "pk":
		*t = IndexTypePrimaryKey
	default:
		return fmt.Errorf("unsupported IndexType: %q", s)
	}

	return nil
}

// Index represents an index on a table.
type Index struct {
	Name        string
	Type        IndexType
	IsUnique    bool
	IsPartial   bool
	ColumnNames []sql.NullString // Column names are NULL if the column is an expression (e.g. a+b)
}

// Indexes returns index information for the given table.
//
// If no such table is found in any of the available databases
// (see Multiple Databases above), Indexes returns an empty
// slice.
func Indexes(db *sql.DB, tableName string) ([]Index, error) {
	return noSchema.Indexes(db, tableName)
}

// Indexes returns index information for the given table.
//
// If no such table is found in this Schema, Indexes returns
// an empty slice.
func (s *Schema) Indexes(db *sql.DB, tableName string) ([]Index, error) {

	placeholder := ""
	params := []interface{}{tableName}

	if s.name != "" {
		placeholder = ", ?"
		params = append(params, s.name, s.name)
	}

	q :=
		`SELECT
			t1.name,
			t1.origin,
			t1."unique",
			t1.partial,
			t2.name
		FROM
			pragma_index_list(?` + placeholder + `) t1
		INNER JOIN
			pragma_index_info(t1.name` + placeholder + `) t2
		ORDER BY
			t1.seq, t2.seqno`

	var rows []struct {
		Name       string
		Type       IndexType
		Unique     bool
		Partial    bool
		ColumnName sql.NullString
	}

	err := queryRows(&rows, db, q, params...)
	if err != nil {
		return nil, fmt.Errorf("could not get indexes for table %s: %s", tableName, err)
	}

	var idx *Index
	var indexes []Index

	for _, r := range rows {

		if idx == nil || r.Name != idx.Name {

			indexes = append(indexes, Index{
				Name:      r.Name,
				Type:      r.Type,
				IsUnique:  r.Unique,
				IsPartial: r.Partial,
			})

			idx = &indexes[len(indexes)-1]
		}

		idx.ColumnNames = append(idx.ColumnNames, r.ColumnName)
	}

	return indexes, nil
}

// TableRankRowID is the TableRank of an IndexColumn that
// represents the ROWID of a table.
const TableRankRowID = -1

// TableRankExpr is the TableRank of an IndexColumn that
// represents an expression rather than a regular table column.
const TableRankExpr = -2

// IndexColumn represents a column in an index.
type IndexColumn struct {
	Name       sql.NullString // NULL if the column is an expression (e.g. a+b)
	Rank       int
	TableRank  int
	Descending bool
	Collation  string
	IsKey      bool
}

// IndexColumns returns column information for the given index.
//
// If no such index is found in any of the available databases
// (see Multiple Databases above), IndexColumns returns an empty
// slice.
func IndexColumns(db *sql.DB, indexName string) ([]IndexColumn, error) {
	return noSchema.IndexColumns(db, indexName)
}

// IndexColumns returns column information for the given index.
//
// If no such index is found in this Schema, IndexColumns returns
// an empty slice.
func (s *Schema) IndexColumns(db *sql.DB, indexName string) ([]IndexColumn, error) {
	return s.indexColumns(db, indexName, false)
}

// IndexColumnsAux returns column information for the given
// index. The difference between this function and IndexColumns
// is that IndexColumnsAux includes any auxiliary columns that
// SQLite inserts into the index.
//
// If no such index is found in any of the available databases
// (see Multiple Databases above), IndexColumnsAux returns an
// empty slice.
func IndexColumnsAux(db *sql.DB, indexName string) ([]IndexColumn, error) {
	return noSchema.IndexColumnsAux(db, indexName)
}

// IndexColumnsAux returns column information for the given
// index. The difference between this method and IndexColumns
// is that IndexColumnsAux includes any auxiliary columns that
// SQLite inserts into the index.
//
// If no such index is found in this Schema, IndexColumnsAux
// returns an empty slice.
func (s *Schema) IndexColumnsAux(db *sql.DB, indexName string) ([]IndexColumn, error) {
	return s.indexColumns(db, indexName, true)
}

func (s *Schema) indexColumns(db *sql.DB, indexName string, includeAux bool) ([]IndexColumn, error) {

	params := []interface{}{indexName}
	if s.name != "" {
		params = append(params, s.name)
	}

	whereClause := "key = 1"
	if includeAux {
		whereClause = "1 = 1"
	}

	q :=
		`SELECT
			name,
			seqno,
			cid,
			desc,
			coll,
			key
		FROM
			pragma_index_xinfo(` + placeholdersFor(params) + `)
		WHERE
			` + whereClause + `
		ORDER BY
			seqno`

	var columns []IndexColumn

	err := queryRows(&columns, db, q, params...)
	if err != nil {
		return nil, fmt.Errorf("could not get columns for index %s: %s", indexName, err)
	}

	return columns, nil
}

func (s *Schema) masterTableNames(db *sql.DB, typ string) ([]string, error) {

	tableName := "sqlite_master"

	if s.name != "" {
		if strings.ToLower(s.name) == "temp" {
			tableName = "sqlite_temp_master"
		} else {
			// Unlike other queries where we're able to use parameters,
			// we insert the user-provided Schema name directly into
			// the SQL here. To protect against SQL injection attacks,
			// we first verify that a database with the given name
			// exists.
			err := s.verify(db)
			if err != nil {
				return nil, fmt.Errorf("could not get %s names: %s", typ, err)
			}
			tableName = s.name + "." + tableName
		}
	}

	q := fmt.Sprintf("SELECT name FROM %s WHERE type = ? ORDER BY name", tableName)

	names, err := queryStrings(db, q, typ)
	if err != nil {
		return nil, fmt.Errorf("could not get %s names: %s", typ, err)
	}

	return names, nil
}

func (s *Schema) verify(db *sql.DB) error {

	var count int
	q := "SELECT COUNT(*) FROM pragma_database_list WHERE LOWER(name) = ?"

	err := db.QueryRow(q, sqlower(s.name)).Scan(&count)
	if err != nil {
		return err
	}

	if count < 1 {
		// Schema doesn't exist. Mimic the error message that
		// SQLite returns in this situation.
		return fmt.Errorf("unknown database '%s'", s.name)
	}

	return nil
}

// sqlower replicates SQLite's LOWER function which converts
// uppercase ASCII characters in a string to lowercase.
func sqlower(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return 'a' + r - 'A'
		}
		return r
	}, s)
}

func toString(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

func placeholdersFor(vals []interface{}) string {
	return strings.Join(strings.Split(strings.Repeat("?", len(vals)), ""), ", ")
}
