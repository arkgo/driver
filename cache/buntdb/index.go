package cache

import (
	"github.com/arkgo/ark"
)

func Driver(ss ...string) ark.CacheDriver {
	store := ""
	if len(ss) > 0 {
		store = ss[0]
	}
	return &fileCacheDriver{store}
}

func init() {
	ark.Driver("buntdb", Driver("store/cache.db"))
	ark.Driver("file", Driver("store/cache.db"))
	ark.Driver("memory", Driver(":memory:"))
}
