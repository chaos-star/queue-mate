package mate

import (
	"fmt"
	"testing"
)

var rabbit = NewRabbit(
	"127.0.0.1",
	5672,
	"admin",
	"admin",
	"/multi",
	30,
	3,
	10,
	nil,
)

type TestConsume struct {
}

func (t *TestConsume) GetOptions() []Option {
	var options []Option
	options = append(options, Option{"test", nil})
	return options
}

func (t *TestConsume) RunConsume(option Option) (err error) {
	client := rabbit.NewClient()
	err = client.Use(t, option).Receive(client.Fanout, "ex_test_exchange", nil, "qx_test_queue")
	return
}

func (t *TestConsume) Process(body []byte, option Option) (err error) {
	fmt.Println("Test Running", string(body), option.Tag)
	return
}

func TestMQBase_Run(t *testing.T) {
	var mqInst = &MQBase{}
	mqInst.Add(new(TestConsume))
	mqInst.Blocking().Run()
}
