package bus_redis

import (
	"strings"
	"sync"
	"time"

	"github.com/arkgo/ark"
	. "github.com/arkgo/asset"
	"github.com/arkgo/asset/util"
	"github.com/gomodule/redigo/redis"
)

//------------------------- 默认队列驱动 begin --------------------------

type (
	redisBusDriver struct{}
	redisBusQueue  struct {
		Thread  int
		Handler ark.QueueHandler
	}
	redisBusConnect struct {
		mutex   sync.RWMutex
		running bool
		actives int64

		name    string
		config  ark.BusConfig
		setting redisBusSetting

		eventHandler ark.EventHandler
		queueHandler ark.QueueHandler

		client *redis.Pool

		events       map[string]ark.EventHandler
		eventStopper *util.Stopper
		eventCloser  string

		queues       map[string]redisBusQueue
		queueStopper *util.Stopper
		queueCloser  string
	}

	redisBusSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}
)

//连接
func (driver *redisBusDriver) Connect(name string, config ark.BusConfig) (ark.BusConnect, error) {

	//获取配置信息
	setting := redisBusSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
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

	// if config.Thread <= 0 {
	// 	config.Thread = 20 //默认100个线程执行队列
	// }

	return &redisBusConnect{
		name: name, config: config, setting: setting,
		events: make(map[string]ark.EventHandler, 0), eventStopper: util.NewStopper(), eventCloser: ark.Unique(config.Prefix),
		queues: make(map[string]redisBusQueue, 0), queueStopper: util.NewStopper(), queueCloser: ark.Unique(config.Prefix),
	}, nil
}

//打开连接
func (connect *redisBusConnect) Open() error {
	connect.client = &redis.Pool{
		MaxIdle: connect.setting.Idle, MaxActive: connect.setting.Active, IdleTimeout: connect.setting.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", connect.setting.Server)
			if err != nil {
				ark.Warning("bus.redis.dial", err)
				return nil, err
			}

			//如果有验证
			if connect.setting.Password != "" {
				if _, err := c.Do("AUTH", connect.setting.Password); err != nil {
					c.Close()
					ark.Warning("bus.redis.auth", err)
					return nil, err
				}
			}
			//如果指定库
			if connect.setting.Database != "" {
				if _, err := c.Do("SELECT", connect.setting.Database); err != nil {
					c.Close()
					ark.Warning("bus.redis.select", err)
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
	return conn.Err()
}
func (connect *redisBusConnect) Health() (ark.BusHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.BusHealth{Workload: connect.actives}, nil
}

//关闭连接
func (connect *redisBusConnect) Close() error {
	if connect.client != nil {

		//结束事件
		connect.Publish(connect.eventCloser, []byte{})
		//结束队列，待优化
		for k, _ := range connect.queues {
			connect.Enqueue(k+connect.queueCloser, []byte{})
		}

		connect.client.Close()
	}
	return nil
}

//注册回调
func (connect *redisBusConnect) Accept(eventHandler ark.EventHandler, queueHandler ark.QueueHandler) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	connect.eventHandler = eventHandler
	connect.queueHandler = queueHandler

	return nil
}

//注册事件
func (connect *redisBusConnect) Event(channel string) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()
	connect.events[channel] = connect.eventHandler
	return nil
}

//注册队列
//待处理，要支持单队列多个线程
func (connect *redisBusConnect) Queue(channel string, thread int) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	if thread <= 0 {
		thread = 1
	}
	connect.queues[channel] = redisBusQueue{thread, connect.queueHandler}

	return nil

	//var queue = connect.queues[channel]
	//
	////开5线程
	//for i := 0; i < thread; i++ {
	//	bus.stopper.RunWorker(func() {
	//		for {
	//			select {
	//			case value := <-queue:
	//				handler(channel, value)
	//			case <-bus.stopper.ShouldStop():
	//				return
	//			}
	//		}
	//	})
	//}

}

func (connect *redisBusConnect) Publish(name string, data []byte, delays ...time.Duration) error {
	if connect.client == nil {
		return ark.Fail
	}

	conn := connect.client.Get()
	defer conn.Close()

	//写入
	realName := connect.config.Prefix + name
	_, err := conn.Do("PUBLISH", realName, string(data))

	if err != nil {
		ark.Warning("bus.redis.publish", err)
		return err
	}

	return nil
}
func (connect *redisBusConnect) Enqueue(name string, data []byte, delays ...time.Duration) error {
	if connect.client == nil {
		return ark.Fail
	}

	conn := connect.client.Get()
	defer conn.Close()

	//写入
	realName := connect.config.Prefix + name
	_, err := conn.Do("LPUSH", realName, string(data))
	if err != nil {
		ark.Warning("bus.redis.enqueue", err)
		return err
	}

	return nil
}

//开始订阅者
func (connect *redisBusConnect) Start() error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	//监听事件
	connect.eventStopper.RunWorker(connect.eventing)
	//监听队列
	for k, v := range connect.queues {
		name := k
		for i := 0; i < v.Thread; i++ {
			connect.queueStopper.RunWorker(func() {
				connect.queueing(name)
			})
		}
	}
	connect.running = true
	return nil
}

//事件监听
func (connect *redisBusConnect) eventing() {

	names := []Any{
		connect.config.Prefix + connect.eventCloser,
	}
	for name, _ := range connect.events {
		names = append(names, connect.config.Prefix+name)
	}

	conn := connect.client.Get()
	defer conn.Close()

	psc := redis.PubSubConn{Conn: conn}
	psc.Subscribe(names...) //一次订阅多个

	for {
		switch rec := psc.Receive().(type) {
		case redis.Message:
			channel := strings.Replace(rec.Channel, connect.config.Prefix, "", 1)
			if channel == connect.eventCloser {
				break //这里退出
			} else {
				if call, ok := connect.events[channel]; ok {
					go call(channel, rec.Data)
				}
			}
		case redis.Subscription:
		case error:
			break
		}
	}

	//取消定
	psc.Unsubscribe(names...)

	//无限循环
	//connect.eventing()
}

//队列监听
//待处理，要支持单队列多个线程
func (connect *redisBusConnect) queueing(name string) {
	names := []Any{
		connect.config.Prefix + name + connect.queueCloser,
		connect.config.Prefix + name,
	}
	//for name, _ := range connect.queues {
	//	names = append(names, connect.config.Prefix+name)
	//}

	//20秒超时
	names = append(names, 10)

	conn := connect.client.Get()
	defer conn.Close()

	for {
		bytes, _ := redis.ByteSlices(conn.Do("BRPOP", names...))
		if bytes != nil && len(bytes) >= 2 {
			channel := strings.Replace(string(bytes[0]), connect.config.Prefix, "", 1)
			data := bytes[1]
			if channel == connect.queueCloser {
				break //退出
			} else {
				if call, ok := connect.queues[channel]; ok {
					call.Handler(channel, data)
				}
			}
		}
	}

	//递归
	//connect.queueing(name)
}

//执行统一到这里
//func (connect *redisBusConnect) serve(name string, value Map) {
//	connect.request("", name, value)
//}

//------------------------- 默认队列驱动 end --------------------------
