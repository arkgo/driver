package cache

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/arkgo/ark"
	"github.com/arkgo/asset/util"
	. "github.com/arkgo/base"
)

type (
	defaultCacheDriver  struct{}
	defaultCacheConnect struct {
		mutex   sync.RWMutex
		name    string
		config  ark.CacheConfig
		setting defaultCacheSetting
		caches  sync.Map
	}
	defaultCacheSetting struct {
		Expiry time.Duration
	}
	defaultCacheValue struct {
		Value  Any
		Expiry time.Time
	}
)

//连接
func (driver *defaultCacheDriver) Connect(name string, config ark.CacheConfig) (ark.CacheConnect, error) {

	setting := defaultCacheSetting{
		Expiry: time.Second * 60,
	}
	if config.Expiry != "" {
		expiry, err := util.ParseDuration(config.Expiry)
		if err == nil {
			setting.Expiry = expiry
		}
	}

	return &defaultCacheConnect{
		name: name, config: config, setting: setting,
		caches: sync.Map{},
	}, nil
}

//打开连接
func (connect *defaultCacheConnect) Open() error {
	return nil
}
func (connect *defaultCacheConnect) Health() (ark.CacheHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.CacheHealth{Workload: 0}, nil
}

//关闭连接
func (connect *defaultCacheConnect) Close() error {
	return nil
}

//查询缓存，
func (connect *defaultCacheConnect) Read(key string) (Any, error) {
	realykey := connect.config.Prefix + key
	if value, ok := connect.caches.Load(realykey); ok {
		if vv, ok := value.(defaultCacheValue); ok {
			if vv.Expiry.Unix() > time.Now().Unix() {
				return vv.Value, nil
			} else {
				//过期了就删除
				connect.Delete(key)
			}
		}
	}
	return nil, errors.New("缓存读取失败")
}

//更新缓存
func (connect *defaultCacheConnect) Write(key string, val Any, expires ...time.Duration) error {
	now := time.Now()

	value := defaultCacheValue{
		Value: val, Expiry: now.Add(connect.setting.Expiry),
	}
	if len(expires) > 0 {
		value.Expiry = now.Add(expires[0])
	}

	realykey := connect.config.Prefix + key
	connect.caches.Store(realykey, value)

	return nil
}

//查询缓存，
func (connect *defaultCacheConnect) Exists(key string) (bool, error) {
	realykey := connect.config.Prefix + key
	if _, ok := connect.caches.Load(realykey); ok {
		return ok, nil
	}
	return false, errors.New("缓存读取失败")
}

//删除缓存
func (connect *defaultCacheConnect) Delete(key string) error {
	realykey := connect.config.Prefix + key
	connect.caches.Delete(realykey)
	return nil
}

func (connect *defaultCacheConnect) Serial(key string, step int64) (int64, error) {
	num := int64(0)

	if vv, err := connect.Read(key); err == nil {
		if vvn, ok := vv.(int64); ok {
			num = vvn
		}
	}

	num += step

	//写入值
	err := connect.Write(key, num)
	if err != nil {
		return int64(0), err
	}

	return num, nil
}

func (connect *defaultCacheConnect) Keys(prefixs ...string) ([]string, error) {
	keys := []string{}
	connect.caches.Range(func(k, v Any) bool {
		key := fmt.Sprintf("%v", k)
		if connect.config.Prefix == "" {
			//没有指定前缀，全部KEY都算进来，还要去掉默认前缀
			key = strings.Replace(key, connect.config.Prefix, "", 1)
		}

		if len(prefixs) == 0 {
			keys = append(keys, key)
		} else {
			for _, pre := range prefixs {
				if strings.HasPrefix(key, pre) {
					keys = append(keys, key)
					break //就算多个pre，只要命中即可
				}
			}
		}
		return true
	})
	return keys, nil
}
func (connect *defaultCacheConnect) Clear(prefixs ...string) error {
	if keys, err := connect.Keys(prefixs...); err == nil {
		for _, key := range keys {
			connect.caches.Delete(key)
		}
		return nil
	} else {
		return err
	}
}
