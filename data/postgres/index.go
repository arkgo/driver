package data_postgres

import (
	"github.com/arkgo/ark"
	_ "github.com/lib/pq" //此包自动注册名为postgres的sql驱动
)

var (
	SCHEMAS = []string{
		"postgresql://",
		"postgres://",
		"pgsql://",
		"pg://",
		"cockroachdb://",
		"cockroach://",
		"crdb://",
	}
	DRIVERS = []string{
		"postgresql", "postgres", "pgsql", "pg",
		"cockroachdb", "cockroach", "crdb",
	}
)

//返回驱动
func Driver() ark.DataDriver {
	return &PostgresDriver{}
}

func init() {
	driver := Driver()
	for _, key := range DRIVERS {
		ark.Register(key, driver)
	}
}
