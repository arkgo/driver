package cache_file

import (
	"github.com/arkgo/ark"
)

func Driver(ss ...string) ark.CacheDriver {
	store := ":memory:"
	if len(ss) > 0 {
		store = ss[0]
	}
	return &fileCacheDriver{store}
}

func init() {
	ark.Driver("buntdb", Driver())
	ark.Driver("memory", Driver())
	ark.Driver("file", Driver(""))
}
