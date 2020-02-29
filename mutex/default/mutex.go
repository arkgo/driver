package mutex

import (
	"errors"
	"sync"
	"time"

	"github.com/arkgo/ark"
)

//默认mutex驱动

type (
	defaultMutexDriver  struct{}
	defaultMutexConnect struct {
		config ark.MutexConfig
		locks  sync.Map
	}
)

func (driver *defaultMutexDriver) Connect(config ark.MutexConfig) (ark.MutexConnect, error) {
	return &defaultMutexConnect{
		config: config, locks: sync.Map{},
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
func (connect *defaultMutexConnect) Lock(key string, expiry time.Duration) error {
	realkey := connect.config.Prefix + key
	if _, exist := connect.locks.LoadOrStore(realkey, true); exist {
		return errors.New("已经存在同名锁")
	}
	return nil
}
func (connect *defaultMutexConnect) Unlock(key string) error {
	realkey := connect.config.Prefix + key
	connect.locks.Delete(realkey)
	return nil
}
