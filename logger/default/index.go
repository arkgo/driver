package logger

import (
	"github.com/arkgo/ark"
)

func Driver(ss ...string) ark.LoggerDriver {
	s := ""
	if len(ss) > 0 {
		s = ss[0]
	}
	return &defaultLoggerDriver{s}
}

func init() {
	ark.Driver("default", Driver())
	ark.Driver("console", Driver())
	ark.Driver("file", Driver("store/logs"))
}
