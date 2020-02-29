package logger

import (
	"os"
	"path"
	"time"

	"github.com/arkgo/ark"
	"github.com/arkgo/asset/log"
	"github.com/arkgo/asset/util"
	. "github.com/arkgo/base"
)

//默认logger驱动

type (
	defaultLoggerDriver struct {
		store string //不为空，就会启用文件存储日志
	}
	defaultLoggerConnect struct {
		config ark.LoggerConfig
		store  string
		logger *log.Logger
	}
)

func (driver *defaultLoggerDriver) Connect(config ark.LoggerConfig) (ark.LoggerConnect, error) {

	store := driver.store
	if vv, ok := config.Setting["store"].(string); ok && vv != "" {
		store = vv
	}

	_, e := os.Stat(store)
	if e != nil {
		//创建目录，如果不存在
		os.MkdirAll(store, 0700)
	}

	return &defaultLoggerConnect{
		config: config, store: store,
	}, nil
}

//打开连接
func (connect *defaultLoggerConnect) Open() error {
	connect.logger = log.NewLogger()

	level := log.LoggerLevel(connect.config.Level)

	if connect.store != "" {
		logFiles := map[int]string{}

		//错误日志
		if vv, ok := connect.config.Setting["error"].(string); ok {
			if vv != "" {
				logFiles[log.LEVEL_ERROR] = vv
			}
		} else if vv, ok := connect.config.Setting["error"].(bool); ok {
			if vv {
				logFiles[log.LEVEL_ERROR] = path.Join(connect.store, "error.log")
			}
		} else {
			logFiles[log.LEVEL_ERROR] = path.Join(connect.store, "error.log")
		}

		if vv, ok := connect.config.Setting["warning"].(string); ok {
			if vv != "" {
				logFiles[log.LEVEL_WARNING] = vv
			}
		} else if vv, ok := connect.config.Setting["warning"].(bool); ok {
			if vv {
				logFiles[log.LEVEL_WARNING] = path.Join(connect.store, "warning.log")
			}
		} else {
			logFiles[log.LEVEL_WARNING] = path.Join(connect.store, "warning.log")
		}

		if vv, ok := connect.config.Setting["info"].(string); ok {
			if vv != "" {
				logFiles[log.LEVEL_INFO] = vv
			}
		} else if vv, ok := connect.config.Setting["info"].(bool); ok {
			if vv {
				logFiles[log.LEVEL_INFO] = path.Join(connect.store, "info.log")
			}
		} else {
			logFiles[log.LEVEL_INFO] = path.Join(connect.store, "info.log")
		}

		if vv, ok := connect.config.Setting["trace"].(string); ok {
			if vv != "" {
				logFiles[log.LEVEL_TRACE] = vv
			}
		} else if vv, ok := connect.config.Setting["trace"].(bool); ok {
			if vv {
				logFiles[log.LEVEL_TRACE] = path.Join(connect.store, "trace.log")
			}
		} else {
			logFiles[log.LEVEL_TRACE] = path.Join(connect.store, "trace.log")
		}

		if vv, ok := connect.config.Setting["debug"].(string); ok {
			if vv != "" {
				logFiles[log.LEVEL_DEBUG] = vv
			}
		} else if vv, ok := connect.config.Setting["debug"].(bool); ok {
			if vv {
				logFiles[log.LEVEL_DEBUG] = path.Join(connect.store, "debug.log")
			}
		} else {
			logFiles[log.LEVEL_DEBUG] = path.Join(connect.store, "debug.log")
		}

		fileConfig := &log.FileConfig{
			// Filename : "logs/test.log",
			LevelFileName: logFiles,
			MaxSize:       1024 * 1024 * 100,
			MaxLine:       1000000,
			DateSlice:     "day",
			Json:          false, Format: "%time% [%type%] %body%",
		}

		if vv, ok := connect.config.Setting["output"].(string); ok {
			if vv != "" {
				fileConfig.Filename = vv
			}
		} else if vv, ok := connect.config.Setting["output"].(bool); ok {
			if vv {
				fileConfig.Filename = path.Join(connect.store, "output.log")
			}
		}

		if connect.config.Format != "" {
			fileConfig.Format = connect.config.Format
		}

		//maxsize
		if vv, ok := connect.config.Setting["maxsize"].(string); ok && vv != "" {
			size := util.ParseSize(vv)
			if size > 0 {
				fileConfig.MaxSize = size
			}
		} else if vv, ok := connect.config.Setting["maxsize"].(int64); ok && vv > 0 {
			fileConfig.MaxSize = vv
		} else if vv, ok := connect.config.Setting["weight"].(int64); ok && vv > 0 {
			fileConfig.MaxSize = vv
		}

		//maxline
		if vv, ok := connect.config.Setting["maxline"].(int64); ok && vv > 0 {
			fileConfig.MaxLine = vv
		} else if vv, ok := connect.config.Setting["height"].(int64); ok && vv > 0 {
			fileConfig.MaxLine = vv
		}

		if vv, ok := connect.config.Setting["slice"].(string); ok && vv != "" {
			fileConfig.DateSlice = log.CheckSlice(vv)
		}

		connect.logger.Attach("file", level, fileConfig)
	}

	if connect.config.Console {
		//是否开启控制台日志
		consoleConfig := &log.ConsoleConfig{
			Json: false, Format: "%time% [%type%] %body%",
		}
		if connect.config.Format != "" {
			consoleConfig.Format = connect.config.Format
		}
		connect.logger.Attach("console", level, consoleConfig)
	}

	//connect.logger.SetAsync()

	// connect.logger = log.NewLogger()

	// logConfig := &log.ConsoleConfig{
	// 	Json: false, Format: "%time% [%type%] %body%",
	// }

	// if connect.config.Format != "" {
	// 	logConfig.Format = connect.config.Format
	// }

	// level := log.LoggerLevel(connect.config.Level)
	// connect.logger.Attach("console", level, logConfig)

	if vv, ok := connect.config.Setting["async"].(bool); ok && vv {
		connect.logger.SetAsync()
	} else if vv, ok := connect.config.Setting["async"].(int); ok {
		connect.logger.SetAsync(vv)
	} else if vv, ok := connect.config.Setting["async"].(int64); ok {
		connect.logger.SetAsync(int(vv))
	}

	return nil
}

func (connect *defaultLoggerConnect) Health() (ark.LoggerHealth, error) {
	// connect.mutex.RLock()
	// defer connect.mutex.RUnlock()
	return ark.LoggerHealth{Workload: 0}, nil
}

//关闭连接
func (connect *defaultLoggerConnect) Close() error {
	//为了最后一条日志能正常输出
	time.Sleep(time.Microsecond * 200)
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
