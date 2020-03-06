package http_default

import (
	"github.com/arkgo/ark"
)

func Driver() ark.HttpDriver {
	return &defaultHttpDriver{}
}

func init() {
	ark.Driver("default", Driver())
}
