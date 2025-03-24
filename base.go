package mate

import (
	"fmt"
	"reflect"
	"sync"
)

type MQConsume interface {
	RunConsume(Option) error
	GetOptions() []Option
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
			fmt.Println(fmt.Sprintf("consume:%v", consume.GetOptions()))
			//消费者MQ对象主协程
			go func(mc MQConsume) {
				var consumeWg = &sync.WaitGroup{}
				for {
					options := consume.GetOptions()
					if len(options) <= 0 {
						options = append(options, Option{"", nil})
					}
					for _, option := range options {
						consumeWg.Add(1)
						go func(mc MQConsume, op Option, wg *sync.WaitGroup) {
							defer wg.Done()
							//断开重试逻辑
							var mcName = reflect.TypeOf(mc).Elem().Name()
							if op.Tag != "" {
								mcName = fmt.Sprintf("%s-%s", mcName, op.Tag)
							}
							m.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] Running...", mcName))
							if err := mc.RunConsume(op); err != nil {
								m.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
							}
						}(consume, option, consumeWg)
					}
					consumeWg.Wait()
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
