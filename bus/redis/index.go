package bus_redis

import (
	"github.com/arkgo/ark"
)

func Driver() ark.BusDriver {
	return &redisBusDriver{}
}

func init() {
	ark.Register("redis", Driver())
}
