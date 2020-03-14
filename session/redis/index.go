package session_redis

import (
	"github.com/arkgo/ark"
)

func Driver() ark.SessionDriver {
	return &redisSessionDriver{}
}

func init() {
	ark.Register("redis", Driver())
}
