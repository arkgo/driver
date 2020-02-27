package logger

import (
	"fmt"
	"strings"

	"github.com/arkgo/ark"
	. "github.com/arkgo/base"
)

func Driver() ark.LoggerDriver {
	return &defaultLoggerDriver{}
}

func init() {
	ark.Driver("default", Driver())
}

func tostring(v Any) string {
	s := ""

	if m, ok := v.(Map); ok {
		vs := []string{}
		for k, v := range m {
			vs = append(vs, k, fmt.Sprintf("%v", v))
		}
		s = strings.Join(vs, " ")
	} else if ms, ok := v.([]Map); ok {
		vs := []string{}
		for _, m := range ms {
			for k, v := range m {
				vs = append(vs, k, fmt.Sprintf("%v", v))
			}
		}
		s = strings.Join(vs, " ")
	} else {
		s = fmt.Sprintf("%v", v)
	}

	return s
}
