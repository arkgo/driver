package session

import (
	"errors"
	"sync"
	"time"

	"github.com/arkgo/ark"
	"github.com/arkgo/asset/util"
	. "github.com/arkgo/base"
)

type (
	defaultSessionDriver  struct{}
	defaultSessionConnect struct {
		name     string
		config   ark.SessionConfig
		setting  defaultSessionSetting
		sessions sync.Map
	}
	defaultSessionSetting struct {
		Expiry time.Duration
	}
	defaultSessionValue struct {
		Value  Map
		Expiry time.Time
	}
)

//连接
func (driver *defaultSessionDriver) Connect(name string, config ark.SessionConfig) (ark.SessionConnect, error) {

	setting := defaultSessionSetting{
		Expiry: time.Hour * 24 * 7,
	}
	if config.Expiry != "" {
		expiry, err := util.ParseDuration(config.Expiry)
		if err == nil {
			setting.Expiry = expiry
		}
	}

	return &defaultSessionConnect{
		name: name, config: config, setting: setting,
		sessions: sync.Map{},
	}, nil
}

//打开连接
func (connect *defaultSessionConnect) Open() error {
	return nil
}
func (connect *defaultSessionConnect) Health() (ark.SessionHealth, error) {
	// connect.mutex.RLock()
	// defer connect.mutex.RUnlock()
	return ark.SessionHealth{Workload: 0}, nil
}

//关闭连接
func (connect *defaultSessionConnect) Close() error {
	return nil
}

//查询会话，
func (connect *defaultSessionConnect) Read(id string) (Map, error) {
	realyid := connect.config.Prefix + id
	if value, ok := connect.sessions.Load(realyid); ok {
		if vv, ok := value.(defaultSessionValue); ok {
			if vv.Expiry.Unix() > time.Now().Unix() {
				return vv.Value, nil
			} else {
				//过期了就删除
				connect.Delete(id)
			}
		}
	}
	return nil, errors.New("会话读取失败")
}

//更新会话
func (connect *defaultSessionConnect) Write(id string, val Map, expires ...time.Duration) error {
	now := time.Now()

	value := defaultSessionValue{
		Value: val, Expiry: now.Add(connect.setting.Expiry),
	}
	if len(expires) > 0 {
		value.Expiry = now.Add(expires[0])
	}

	realyid := connect.config.Prefix + id
	connect.sessions.Store(realyid, value)

	return nil
}

//删除会话
func (connect *defaultSessionConnect) Delete(id string) error {
	realyid := connect.config.Prefix + id
	connect.sessions.Delete(realyid)
	return nil
}

//清空会话
func (connect *defaultSessionConnect) Clear() error {
	connect.sessions.Range(func(k, v Any) bool {
		connect.sessions.Delete(k)
		return true
	})
	return nil
}
