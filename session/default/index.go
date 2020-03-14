package session

import (
	"github.com/arkgo/ark"
)

func Driver() ark.SessionDriver {
	return &defaultSessionDriver{}
}

func init() {
	ark.Register("default", Driver())
}
