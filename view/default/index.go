package view_default

import (
	"github.com/arkgo/ark"
)

func Driver() ark.ViewDriver {
	return &defaultViewDriver{}
}

func init() {
	ark.Driver("default", Driver())
}
