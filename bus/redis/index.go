package bus_redis

import (
	"arkgo/ark"
)

func Driver() ark.BusDriver {
	return &redisBusDriver{}
}

func init() {
	ark.Driver("redis", Driver())
}
