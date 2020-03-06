package cache_redis

import (
	"errors"
	"sync"
	"time"

	"github.com/arkgo/ark"
	"github.com/arkgo/asset/util"
	. "github.com/arkgo/base"

	"github.com/gomodule/redigo/redis"
)

//-------------------- redisCacheBase begin -------------------------

type (
	redisCacheDriver  struct{}
	redisCacheConnect struct {
		mutex   sync.RWMutex
		actives int64

		name    string
		config  ark.CacheConfig
		setting redisCacheSetting

		client *redis.Pool
	}
	redisCacheSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库
		Expiry   time.Duration

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}

	redisCacheValue struct {
		Value Any `json:"value"`
	}
)

//连接
func (driver *redisCacheDriver) Connect(name string, config ark.CacheConfig) (ark.CacheConnect, error) {

	//获取配置信息
	setting := redisCacheSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
		Expiry: time.Hour, //默认1小时有效
	}

	//默认超时时间
	if config.Expiry != "" {
		td, err := util.ParseDuration(config.Expiry)
		if err == nil {
			setting.Expiry = td
		}
	}

	if vv, ok := config.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if vv, ok := config.Setting["password"].(string); ok && vv != "" {
		setting.Password = vv
	}

	//数据库，redis的0-16号
	if v, ok := config.Setting["database"].(string); ok {
		setting.Database = v
	}

	if vv, ok := config.Setting["idle"].(int64); ok && vv > 0 {
		setting.Idle = int(vv)
	}
	if vv, ok := config.Setting["active"].(int64); ok && vv > 0 {
		setting.Active = int(vv)
	}
	if vv, ok := config.Setting["timeout"].(int64); ok && vv > 0 {
		setting.Timeout = time.Second * time.Duration(vv)
	}
	if vv, ok := config.Setting["timeout"].(string); ok && vv != "" {
		td, err := util.ParseDuration(vv)
		if err == nil {
			setting.Timeout = td
		}
	}

	return &redisCacheConnect{
		name: name, config: config, setting: setting,
	}, nil
}

//打开连接
func (connect *redisCacheConnect) Open() error {
	connect.client = &redis.Pool{
		MaxIdle: connect.setting.Idle, MaxActive: connect.setting.Active, IdleTimeout: connect.setting.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", connect.setting.Server)
			if err != nil {
				ark.Warning("session.redis.dial", err)
				return nil, err
			}

			//如果有验证
			if connect.setting.Password != "" {
				if _, err := c.Do("AUTH", connect.setting.Password); err != nil {
					c.Close()
					ark.Warning("session.redis.auth", err)
					return nil, err
				}
			}
			//如果指定库
			if connect.setting.Database != "" {
				if _, err := c.Do("SELECT", connect.setting.Database); err != nil {
					c.Close()
					ark.Warning("session.redis.select", err)
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	//打开一个试一下
	conn := connect.client.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	return nil
}
func (connect *redisCacheConnect) Health() (ark.CacheHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.CacheHealth{Workload: connect.actives}, nil
}

//关闭连接
func (connect *redisCacheConnect) Close() error {
	if connect.client != nil {
		if err := connect.client.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (connect *redisCacheConnect) Serial(key string, start, step int64) (int64, error) { //加并发锁
	//加锁可以
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	value := start

	if val, err := connect.Read(key); err == nil {
		if vv, ok := val.(float64); ok {
			value = int64(vv)
		} else if vv, ok := val.(int64); ok {
			value = vv
		}
	}

	//加数字
	value += step

	//写入值
	err := connect.Write(key, value, 0)
	if err != nil {
		return int64(0), err
	}

	return value, nil
}

//查询缓存，
func (connect *redisCacheConnect) Exists(key string) (bool, error) {
	if connect.client == nil {
		return false, errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	realKey := connect.config.Prefix + key

	exists, err := redis.Int(conn.Do("EXISTS", realKey))
	if err != nil {
		return false, err
	}

	if exists > 0 {
		return true, nil
	}

	return false, nil
}

//查询缓存，
func (connect *redisCacheConnect) Read(key string) (Any, error) {
	if connect.client == nil {
		return nil, errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	realKey := connect.config.Prefix + key

	val, err := redis.Bytes(conn.Do("GET", realKey))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	realVal := redisCacheValue{}
	err = ark.Unmarshal(val, &realVal)
	if err != nil {
		return nil, err
	}

	return realVal.Value, nil
}

//更新缓存
func (connect *redisCacheConnect) Write(key string, val Any, expires ...time.Duration) error {
	if connect.client == nil {
		return errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	realKey := connect.config.Prefix + key
	realVal := redisCacheValue{val}

	expiry := connect.setting.Expiry
	if len(expires) > 0 {
		expiry = expires[0]
	}

	bytes, err := ark.Marshal(realVal)
	if err != nil {
		return err
	}

	args := []Any{
		realKey, string(bytes),
	}
	if expiry > 0 {
		args = append(args, "EX", expiry.Seconds())
	}

	_, err = conn.Do("SET", args...)
	if err != nil {
		return err
	}

	return nil
}

//删除缓存
func (connect *redisCacheConnect) Delete(key string) error {

	if connect.client == nil {
		return errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	realKey := connect.config.Prefix + key

	_, err := conn.Do("DEL", realKey)
	if err != nil {
		return err
	}
	return nil
}

func (connect *redisCacheConnect) Clear(prefixs ...string) error {
	if connect.client == nil {
		return errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	keys, err := connect.Keys(prefixs...)
	if err != nil {
		return err
	}

	for _, key := range keys {
		_, err := conn.Do("DEL", key)
		if err != nil {
			return err
		}
	}

	return nil
}
func (connect *redisCacheConnect) Keys(prefixs ...string) ([]string, error) {
	keys := []string{}

	if connect.client == nil {
		return nil, errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	if len(prefixs) > 0 {
		for _, prefix := range prefixs {
			alls, _ := redis.Strings(conn.Do("KEYS", connect.config.Prefix+prefix+"*"))
			for _, key := range alls {
				keys = append(keys, key)
			}
		}
	} else {
		alls, _ := redis.Strings(conn.Do("KEYS", connect.config.Prefix+"*"))
		for _, key := range alls {
			keys = append(keys, key)
		}
	}

	return keys, nil
}

//-------------------- redisCacheBase end -------------------------
