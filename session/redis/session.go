package session_redis

import (
	"errors"
	"time"

	"github.com/arkgo/ark"
	"github.com/arkgo/asset/util"
	. "github.com/arkgo/base"
	"github.com/gomodule/redigo/redis"
)

type (
	redisSessionDriver  struct{}
	redisSessionConnect struct {
		name    string
		config  ark.SessionConfig
		setting redisSessionSetting

		client *redis.Pool
	}
	//配置文件
	redisSessionSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库
		Expiry   time.Duration

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}
)

//连接
func (driver *redisSessionDriver) Connect(name string, config ark.SessionConfig) (ark.SessionConnect, error) {

	//获取配置信息
	setting := redisSessionSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
		Expiry: time.Hour * 24 * 7, //默认7天有效
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

	return &redisSessionConnect{
		name: name, config: config, setting: setting,
	}, nil
}

//打开连接
func (connect *redisSessionConnect) Open() error {
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
func (connect *redisSessionConnect) Health() (ark.SessionHealth, error) {
	// connect.mutex.RLock()
	// defer connect.mutex.RUnlock()
	return ark.SessionHealth{Workload: 0}, nil
}

//关闭连接
func (connect *redisSessionConnect) Close() error {
	if connect.client != nil {
		if err := connect.client.Close(); err != nil {
			return err

		}
	}
	return nil
}

//查询会话，
func (connect *redisSessionConnect) Read(id string) (Map, error) {

	if connect.client == nil {
		return nil, errors.New("连接失败")
	}

	conn := connect.client.Get()
	defer conn.Close()

	key := connect.config.Prefix + id
	val, err := redis.String(conn.Do("GET", key))
	if err != nil {
		return nil, err
	}

	m := Map{}
	err = ark.Unmarshal([]byte(val), &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

//更新会话
func (connect *redisSessionConnect) Write(id string, value Map, expires ...time.Duration) error {

	if connect.client == nil {
		return errors.New("连接失败")
	}

	conn := connect.client.Get()
	defer conn.Close()

	//带前缀
	key := connect.config.Prefix + id

	//JSON解析
	bytes, err := ark.Marshal(value)
	if err != nil {
		return err
	}

	expiry := connect.setting.Expiry
	if len(expires) > 0 {
		expiry = expires[0]
	}

	args := []Any{
		key, string(bytes),
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

//删除会话
func (connect *redisSessionConnect) Delete(id string) error {
	if connect.client == nil {
		return errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	//key要加上前缀
	key := connect.config.Prefix + id

	_, err := conn.Do("DEL", key)
	if err != nil {
		return err
	}

	return nil
}

//删除会话
func (connect *redisSessionConnect) Clear() error {
	if connect.client == nil {
		return errors.New("连接失败")
	}
	conn := connect.client.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", connect.config.Prefix+"*"))
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
