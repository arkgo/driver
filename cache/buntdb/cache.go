package cache

import (
	"errors"
	"sync"
	"time"

	"github.com/arkgo/ark"
	"github.com/arkgo/asset/util"
	. "github.com/arkgo/base"
	"github.com/tidwall/buntdb"
)

//-------------------- fileCacheBase begin -------------------------

type (
	fileCacheDriver struct {
		store string
	}
	fileCacheConnect struct {
		mutex sync.RWMutex

		name    string
		config  ark.CacheConfig
		setting fileCacheSetting

		db *buntdb.DB
	}
	fileCacheSetting struct {
		Store  string
		Expiry time.Duration
	}
	fileCacheValue struct {
		Value Any `json:"value"`
	}
)

//连接
func (driver *fileCacheDriver) Connect(name string, config ark.CacheConfig) (ark.CacheConnect, error) {

	//获取配置信息
	setting := fileCacheSetting{
		Store:  driver.store,
		Expiry: time.Second * 60,
	}

	//默认超时时间
	if config.Expiry != "" {
		td, err := util.ParseDuration(config.Expiry)
		if err == nil {
			setting.Expiry = td
		}
	}

	if vv, ok := config.Setting["file"].(string); ok && vv != "" {
		setting.Store = vv
	}
	if vv, ok := config.Setting["store"].(string); ok && vv != "" {
		setting.Store = vv
	}

	return &fileCacheConnect{
		name: name, config: config, setting: setting,
	}, nil
}

//打开连接
func (connect *fileCacheConnect) Open() error {
	if connect.setting.Store == "" {
		return errors.New("无效缓存存储")
	}
	db, err := buntdb.Open(connect.setting.Store)
	if err != nil {
		return err
	}
	connect.db = db
	return nil
}
func (connect *fileCacheConnect) Health() (ark.CacheHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.CacheHealth{Workload: 0}, nil
}

//关闭连接
func (connect *fileCacheConnect) Close() error {
	if connect.db != nil {
		if err := connect.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

//查询缓存，
func (connect *fileCacheConnect) Read(key string) (Any, error) {
	if connect.db == nil {
		return nil, errors.New("[缓存]连接失败")
	}

	realKey := connect.config.Prefix + key
	realVal := ""

	err := connect.db.View(func(tx *buntdb.Tx) error {
		vvv, err := tx.Get(realKey)
		if err != nil {
			return err
		}
		realVal = vvv
		return nil
	})
	if err != nil {
		return nil, err
	}

	mcv := fileCacheValue{}
	err = ark.Unmarshal([]byte(realVal), &mcv)
	if err != nil {
		return nil, nil
	}

	return mcv.Value, nil
}

//更新缓存
func (connect *fileCacheConnect) Write(key string, val Any, expires ...time.Duration) error {
	if connect.db == nil {
		return errors.New("[缓存]连接失败")
	}

	value := fileCacheValue{val}

	//JSON解析
	bytes, err := ark.Marshal(value)
	if err != nil {
		return err
	}

	realKey := connect.config.Prefix + key
	realVal := string(bytes)

	expiry := connect.setting.Expiry
	if len(expires) > 0 {
		expiry = expires[0]
	}

	return connect.db.Update(func(tx *buntdb.Tx) error {
		opts := &buntdb.SetOptions{Expires: false}
		if expiry > 0 {
			opts.Expires = true
			opts.TTL = expiry
		}
		_, _, err := tx.Set(realKey, realVal, opts)
		return err
	})
}

//查询缓存，
func (connect *fileCacheConnect) Exists(key string) (bool, error) {
	if connect.db == nil {
		return false, errors.New("[缓存]连接失败")
	}

	realKey := connect.config.Prefix + key

	err := connect.db.View(func(tx *buntdb.Tx) error {
		_, err := tx.Get(realKey)
		return err
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return true, nil
		}
	}
	return false, nil
}

//删除缓存
func (connect *fileCacheConnect) Delete(key string) error {
	if connect.db == nil {
		return errors.New("连接失败")
	}

	//key要加上前缀
	realKey := connect.config.Prefix + key
	return connect.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(realKey)
		return err
	})
}

func (connect *fileCacheConnect) Serial(key string, step int64) (int64, error) {
	if connect.db == nil {
		return int64(0), errors.New("连接失败")
	}

	value := int64(0)
	val, err := connect.Read(key)
	if err != nil {
		return int64(0), err
	}
	if vv, ok := val.(float64); ok {
		value = int64(vv)
	} else if vv, ok := val.(int64); ok {
		value = vv
	}

	//加数字
	value += step

	//写入值，这个应该不过期
	err = connect.Write(key, value, 0)
	if err != nil {
		return int64(0), err
	}

	return value, nil
}

func (connect *fileCacheConnect) Clear(prefixs ...string) error {
	if connect.db == nil {
		return errors.New("连接失败")
	}

	keys, err := connect.Keys(prefixs...)
	if err != nil {
		return err
	}

	return connect.db.Update(func(tx *buntdb.Tx) error {
		for _, key := range keys {
			_, err := tx.Delete(key)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
func (connect *fileCacheConnect) Keys(prefixs ...string) ([]string, error) {
	if connect.db == nil {
		return nil, errors.New("连接失败")
	}

	keys := []string{}
	err := connect.db.View(func(tx *buntdb.Tx) error {
		if len(prefixs) > 0 {
			for _, prefix := range prefixs {
				tx.AscendKeys(connect.config.Prefix+prefix+"*", func(k, v string) bool {
					keys = append(keys, k)
					return true
				})
			}
		} else {
			tx.AscendKeys(connect.config.Prefix+"*", func(k, v string) bool {
				keys = append(keys, k)
				return true
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

//-------------------- fileCacheBase end -------------------------
