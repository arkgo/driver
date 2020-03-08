package mutex

import (
	"errors"
	"sync"
	"time"

	"github.com/arkgo/ark"
	"github.com/arkgo/asset/util"
)

//默认mutex驱动

type (
	defaultMutexDriver  struct{}
	defaultMutexConnect struct {
		name string
		config  ark.MutexConfig
		setting defaultMutexSetting
		locks   sync.Map
	}
	defaultMutexSetting struct {
		Expiry time.Duration
	}
	defaultMutexValue struct {
		Expiry time.Time
	}
)

func (driver *defaultMutexDriver) Connect(name string, config ark.MutexConfig) (ark.MutexConnect, error) {

	setting := defaultMutexSetting{
		Expiry: time.Second * 3,
	}
	if config.Expiry != "" {
		expiry, err := util.ParseDuration(config.Expiry)
		if err == nil {
			setting.Expiry = expiry
		}
	}

	return &defaultMutexConnect{
		name: name, config: config, setting: setting,
	}, nil
}

//打开连接
func (connect *defaultMutexConnect) Open() error {
	return nil
}

func (connect *defaultMutexConnect) Health() (ark.MutexHealth, error) {
	return ark.MutexHealth{Workload: 0}, nil
}

//关闭连接
func (connect *defaultMutexConnect) Close() error {
	return nil
}

//待优化，加上超时设置
func (connect *defaultMutexConnect) Lock(key string, expires ...time.Duration) error {
	now := time.Now()

	realkey := connect.config.Prefix + key

	if vv, ok := connect.locks.Load(realkey); ok {
		if tm, ok := vv.(defaultMutexValue); ok {
			if tm.Expiry.Unix() > now.Unix() {
				return errors.New("已经存在同名锁")
			}
		}
	}

	value := defaultMutexValue{
		Expiry: now.Add(connect.setting.Expiry),
	}
	if len(expires) > 0 {
		value.Expiry = now.Add(expires[0])
	}

	connect.locks.Store(realkey, value)

	return nil
}
func (connect *defaultMutexConnect) Unlock(key string) error {
	realkey := connect.config.Prefix + key
	connect.locks.Delete(realkey)
	return nil
}
