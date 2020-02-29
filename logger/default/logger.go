package logger

import (
	"github.com/arkgo/ark"
	"github.com/arkgo/asset/log"
	. "github.com/arkgo/base"
)

//默认logger驱动

type (
	defaultLoggerDriver  struct{}
	defaultLoggerConnect struct {
		config ark.LoggerConfig
		logger *log.Logger
	}
)

func (driver *defaultLoggerDriver) Connect(config ark.LoggerConfig) (ark.LoggerConnect, error) {
	return &defaultLoggerConnect{
		config: config,
	}, nil
}

//打开连接
func (connect *defaultLoggerConnect) Open() error {
	connect.logger = log.NewLogger()

	logConfig := &log.ConsoleConfig{
		Json: false, Format: "%time% [%type%] %body%",
	}

	if connect.config.Format != "" {
		logConfig.Format = connect.config.Format
	}

	level := log.LoggerLevel(connect.config.Level)
	connect.logger.Attach("console", level, logConfig)

	// connect.logger.SetAsync()

	return nil
}

func (connect *defaultLoggerConnect) Health() (ark.LoggerHealth, error) {
	// connect.mutex.RLock()
	// defer connect.mutex.RUnlock()
	return ark.LoggerHealth{Workload: 0}, nil
}

//关闭连接
func (connect *defaultLoggerConnect) Close() error {
	connect.logger.Flush()
	return nil
}

func (connect *defaultLoggerConnect) Debug(body string) {
	connect.logger.Debug(body)
}
func (connect *defaultLoggerConnect) Debugf(format string, args ...Any) {
	connect.logger.Debugf(format, args...)
}

func (connect *defaultLoggerConnect) Trace(body string) {
	connect.logger.Trace(body)
}
func (connect *defaultLoggerConnect) Tracef(format string, args ...Any) {
	connect.logger.Tracef(format, args...)
}

func (connect *defaultLoggerConnect) Info(body string) {
	connect.logger.Info(body)
}
func (connect *defaultLoggerConnect) Infof(format string, args ...Any) {
	connect.logger.Infof(format, args...)
}

func (connect *defaultLoggerConnect) Warning(body string) {
	connect.logger.Warning(body)
}

func (connect *defaultLoggerConnect) Warningf(format string, args ...Any) {
	connect.logger.Warningf(format, args...)
}

func (connect *defaultLoggerConnect) Error(body string) {
	connect.logger.Error(body)
}
func (connect *defaultLoggerConnect) Errorf(format string, args ...Any) {
	connect.logger.Errorf(format, args...)
}
