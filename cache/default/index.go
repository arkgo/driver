package cache

import (
	"github.com/arkgo/ark"
)

func Driver() ark.CacheDriver {
	return &defaultCacheDriver{}
}

func init() {
	ark.Driver("default", Driver())
}
