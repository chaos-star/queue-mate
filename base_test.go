package mate

import (
	"errors"
	"fmt"
	"testing"
)

var rabbit = NewRabbit(
	"172.16.110.205",
	5672,
	"guest",
	"2LrEN5tCHhY8k4gs",
	"/multi",
	30,
	1,
	30,
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
	err = client.Use(t, option).Retry(3).ConsumerNum(3).Receive(client.Fanout, "ex_test_exchange", nil, "qx_test_queue")
	return
}

func (t *TestConsume) Process(body []byte, option Option) (err error) {
	fmt.Println("Test Running", string(body), option.Tag)
	err = errors.New("this is an error")
	return
}

func TestMQBase_Run(t *testing.T) {
	var mqInst = &MQBase{}
	mqInst.Add(new(TestConsume))
	mqInst.Blocking().Run()
}
