package bus

import (
	"sync"
	"time"

	"github.com/arkgo/ark"
)

//------------------------- 默认队列驱动 begin --------------------------

type (
	defaultBusDriver  struct{}
	defaultBusConnect struct {
		mutex   sync.RWMutex
		running bool
		actives int64

		name   string
		config ark.BusConfig

		eventHandler ark.EventHandler
		queueHandler ark.QueueHandler
	}
)

//连接
func (driver *defaultBusDriver) Connect(name string, config ark.BusConfig) (ark.BusConnect, error) {
	return &defaultBusConnect{
		name: name, config: config,
	}, nil
}

//打开连接
func (connect *defaultBusConnect) Open() error {
	return nil
}
func (connect *defaultBusConnect) Health() (ark.BusHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.BusHealth{Workload: connect.actives}, nil
}

//关闭连接
func (connect *defaultBusConnect) Close() error {
	bus.stopper.Stop()
	return nil
}

func (connect *defaultBusConnect) Accept(eventHandler ark.EventHandler, queueHandler ark.QueueHandler) error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	connect.eventHandler = eventHandler
	connect.queueHandler = queueHandler

	return nil
}

func (connect *defaultBusConnect) Event(channel string) error {
	return bus.Event(channel, connect.eventHandler)
}
func (connect *defaultBusConnect) Queue(channel string, thread int) error {
	if thread <= 0 {
		thread = 1
	}
	return bus.Queue(channel, thread, connect.queueHandler)
}

//开始订阅者
func (connect *defaultBusConnect) Start() error {
	connect.mutex.Lock()
	defer connect.mutex.Unlock()

	connect.running = true
	return nil
}

func (connect *defaultBusConnect) Publish(name string, data []byte, delays ...time.Duration) error {
	if len(delays) > 0 {
		time.AfterFunc(delays[0], func() {
			bus.Publish(name, data)
		})
	} else {
		return bus.Publish(name, data)
	}
	return nil
}
func (connect *defaultBusConnect) Enqueue(name string, data []byte, delays ...time.Duration) error {
	if len(delays) > 0 {
		time.AfterFunc(delays[0], func() {
			bus.Enqueue(name, data)
		})
	} else {
		return bus.Enqueue(name, data)
	}
	return nil
}

//执行统一到这里
//func (connect *defaultBusConnect) serve(name string, value Map) {
//	connect.request("", name, value)
//}

//------------------------- 默认队列驱动 end --------------------------
