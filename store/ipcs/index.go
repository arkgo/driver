package store_ipcs

import (
	"github.com/arkgo/ark"
)

func Driver() ark.StoreDriver {
	return &ipcsStoreDriver{}
}

func init() {
	ark.Register("ipcs", Driver())
}
