package store_default

import (
	"github.com/arkgo/ark"
)

func Driver() ark.StoreDriver {
	return &defaultStoreDriver{}
}

func init() {
	ark.Register("default", Driver())
}
