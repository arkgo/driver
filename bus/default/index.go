package bus

import (
	"github.com/arkgo/ark"
)

func Driver() ark.BusDriver {
	return &defaultBusDriver{}
}

func init() {
	ark.Register("default", Driver())
}
