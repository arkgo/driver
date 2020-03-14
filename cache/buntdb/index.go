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
	ark.Register("buntdb", Driver("store/cache.db"))
	ark.Register("file", Driver("store/cache.db"))
	ark.Register("memory", Driver(":memory:"))
}
