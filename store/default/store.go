package store_default

import (
	"os"
	"sync"
	"time"

	"github.com/arkgo/ark"
	. "github.com/arkgo/asset"
)

//-------------------- defaultStoreBase begin -------------------------

type (
	defaultStoreDriver  struct{}
	defaultStoreConnect struct {
		mutex   sync.RWMutex
		actives int64

		name   string
		config ark.StoreConfig
	}
)

//连接
func (driver *defaultStoreDriver) Connect(name string, config ark.StoreConfig) (ark.StoreConnect, error) {

	if config.Cache == "" {
		config.Cache = os.TempDir()
	} else {
		if _, err := os.Stat(config.Cache); err != nil {
			os.MkdirAll(config.Cache, 0777)
		}
	}

	return &defaultStoreConnect{
		actives: int64(0),
		name:    name, config: config,
	}, nil

}

//打开连接
func (connect *defaultStoreConnect) Open() error {
	return nil
}

func (connect *defaultStoreConnect) Health() (ark.StoreHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.StoreHealth{Workload: connect.actives}, nil
}

//关闭连接
func (connect *defaultStoreConnect) Close() error {
	return nil
}

func (connect *defaultStoreConnect) Upload(target string, metadata Map) (ark.File, ark.Files, error) {
	//使用系统本身的文件存储
	return ark.Storage(target)
}

func (connect *defaultStoreConnect) Download(file ark.File) (string, error) {
	return ark.Download(file.Code())
}

func (connect *defaultStoreConnect) Remove(file ark.File) error {
	return ark.Remove(file.Code())
}

func (connect *defaultStoreConnect) Browse(file ark.File, name string, expiries ...time.Duration) (string, error) {
	return ark.Browse(file.Code(), name, expiries...), nil
}

func (connect *defaultStoreConnect) Preview(file ark.File, w, h, t int64, expiries ...time.Duration) (string, error) {
	return ark.Preview(file.Code(), w, h, t, expiries...), nil
}

//-------------------- defaultStoreBase end -------------------------
