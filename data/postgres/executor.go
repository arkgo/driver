package data_postgres

import (
	"database/sql"

	. "github.com/arkgo/asset"
)

type (
	PostgresExecutor interface {
		Exec(query string, args ...Any) (sql.Result, error)
		Prepare(query string) (*sql.Stmt, error)
		Query(query string, args ...Any) (*sql.Rows, error)
		QueryRow(query string, args ...Any) *sql.Row
	}
)
