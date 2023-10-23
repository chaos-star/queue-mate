package mate

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

type MQConsume interface {
	RunConsume() error
	GetTag() string
}

type MQBase struct {
	consumes []MQConsume
	log      Logger
	blocking bool
}

func (m *MQBase) With(log Logger) *MQBase {
	m.log = log
	return m
}

func (m *MQBase) Blocking() *MQBase {
	m.blocking = true
	return m
}

func (m *MQBase) Add(consumes ...MQConsume) {
	m.consumes = append(m.consumes, consumes...)
}

func (m *MQBase) Run() {
	if len(m.consumes) > 0 {
		if m.log == nil {
			m.log = new(ConsoleOutput)
		}
		for _, consume := range m.consumes {
			//消费者MQ对象主协程
			go func(mc MQConsume) {
				//断开重试逻辑
				var (
					wg     = &sync.WaitGroup{}
					mcName = reflect.TypeOf(mc).Elem().Name()
					mcTag  = mc.GetTag()
				)
				if mcTag != "" {
					mcName = fmt.Sprintf("%s-%s", mcName, mc.GetTag())
				}
				for {
					wg.Add(1)
					//消费者子协程Panic,Err 退出后重启
					go func(wg *sync.WaitGroup) {
						defer func() {
							//处理Panic
							if x := recover(); x != nil {
								m.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [PANIC] [RUN] Exception:%#v", mcName, x))
							}
							wg.Done()
						}()
						//启动消费者协程
						m.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] Running...", mcName))
						err := mc.RunConsume()
						if err != nil {
							m.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
						}
						//休眠 10s 重试
						time.Sleep(15 * time.Second)
						return
					}(wg)
					wg.Wait()
					m.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] Restart...", mcName))
				}
			}(consume)
		}
		fmt.Println("MQ Queue Run Success, press CTRL + C exit.")
		if m.blocking {
			select {}
		}
	} else {
		fmt.Println("No execution queue available")
	}
}
