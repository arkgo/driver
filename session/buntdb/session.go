package session

import (
	"errors"
	"sync"
	"time"

	"github.com/arkgo/ark"
	. "github.com/arkgo/asset"
	"github.com/arkgo/asset/util"
	"github.com/tidwall/buntdb"
)

//-------------------- fileSessionBase begin -------------------------

type (
	fileSessionDriver struct {
		store string
	}
	fileSessionConnect struct {
		mutex sync.RWMutex

		name    string
		config  ark.SessionConfig
		setting fileSessionSetting

		db *buntdb.DB
	}
	fileSessionSetting struct {
		Store  string
		Expiry time.Duration
	}
	fileSessionValue struct {
		Value Any `json:"value"`
	}
)

//连接
func (driver *fileSessionDriver) Connect(name string, config ark.SessionConfig) (ark.SessionConnect, error) {

	//获取配置信息
	setting := fileSessionSetting{
		Store:  driver.store,
		Expiry: time.Hour * 24 * 7,
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

	return &fileSessionConnect{
		name: name, config: config, setting: setting,
	}, nil
}

//打开连接
func (connect *fileSessionConnect) Open() error {
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
func (connect *fileSessionConnect) Health() (ark.SessionHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.SessionHealth{Workload: 0}, nil
}

//关闭连接
func (connect *fileSessionConnect) Close() error {
	if connect.db != nil {
		if err := connect.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

//查询缓存，
func (connect *fileSessionConnect) Read(id string) (Map, error) {
	if connect.db == nil {
		return nil, errors.New("[会话]连接失败")
	}

	realKey := connect.config.Prefix + id
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

	value := Map{}
	err = ark.Unmarshal([]byte(realVal), &value)
	if err != nil {
		return nil, nil
	}

	return value, nil
}

//更新缓存
func (connect *fileSessionConnect) Write(key string, val Map, expires ...time.Duration) error {
	if connect.db == nil {
		return errors.New("[会话]连接失败")
	}

	//JSON解析
	bytes, err := ark.Marshal(val)
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

//删除缓存
func (connect *fileSessionConnect) Delete(key string) error {
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

func (connect *fileSessionConnect) Clear() error {
	if connect.db == nil {
		return errors.New("连接失败")
	}

	return connect.db.Update(func(tx *buntdb.Tx) error {
		return tx.DeleteAll()
	})
}
