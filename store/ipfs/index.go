package store_ipfs

import (
	"github.com/arkgo/ark"
)

func Driver() ark.StoreDriver {
	return &ipfsStoreDriver{}
}

func init() {
	ark.Register("ipfs", Driver())
}
