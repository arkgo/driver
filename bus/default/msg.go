package bus

import (
	"sync"

	"github.com/arkgo/ark"
	"github.com/arkgo/asset/util"
	. "github.com/arkgo/base"
)

//------------------------- 默认队列驱动 begin --------------------------

type (
	defaultBus struct {
		mutex   sync.Mutex
		stopper *util.Stopper
		events  map[string][]ark.BusHandler
		queues  map[string]chan []byte
	}
	defaultBusFunc func(string, Map)
)

var (
	bus *defaultBus
)

func init() {
	bus = &defaultBus{stopper: util.NewStopper(), events: make(map[string][]ark.BusHandler, 0), queues: make(map[string]chan []byte, 0)}
}

//订阅事件
func (bus *defaultBus) Event(channel string, handler ark.BusHandler) error {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	if _, ok := bus.events[channel]; ok == false {
		bus.events[channel] = []ark.BusHandler{}
	}

	//加入调用列表
	bus.events[channel] = append(bus.events[channel], handler)

	return nil
}

//订阅队列
func (bus *defaultBus) Queue(channel string, thread int, handler ark.BusHandler) error {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	if _, ok := bus.queues[channel]; ok == false {
		bus.queues[channel] = make(chan []byte)
	}

	var queue = bus.queues[channel]

	//开5线程
	for i := 0; i < thread; i++ {
		bus.stopper.RunWorker(func() {
			for {
				select {
				case value := <-queue:
					handler(channel, value)
				case <-bus.stopper.ShouldStop():
					return
				}
			}
		})
	}

	return nil
}

//发布消息，可以N多线程，
func (busMsg *defaultBus) Publish(channel string, data []byte) error {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	if calls, ok := bus.events[channel]; ok {
		for _, call := range calls {
			go call(channel, data)
		}
	}

	return nil
}

//发起队列，限制线程
func (bus *defaultBus) Enqueue(channel string, data []byte) error {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	//这里不能阻塞线程
	if cc, ok := bus.queues[channel]; ok {
		go func() {
			cc <- data
		}()
	}

	return nil
}

//------------------------- 默认队列驱动 end --------------------------
