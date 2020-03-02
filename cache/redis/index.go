package cache_redis

import (
	"github.com/arkgo/ark"
)

func Driver() ark.CacheDriver {
	return &redisCacheDriver{}
}

func init() {
	ark.Driver("redis", Driver())
}
