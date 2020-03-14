package mutex

import (
	"github.com/arkgo/ark"
)

func Driver() ark.MutexDriver {
	return &defaultMutexDriver{}
}

func init() {
	ark.Register("default", Driver())
}
