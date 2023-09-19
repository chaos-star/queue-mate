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

func (t *TestConsume) RunConsume() (err error) {
	client := rabbit.NewClient()
	err = client.Use(t).Receive(client.Fanout, "ex_test_exchange", nil, "qx_test_queue")
	return
}

func (t *TestConsume) Process(body []byte) (err error) {
	fmt.Println("Test Running", string(body))
	return
}

func TestMQBase_Run(t *testing.T) {
	var mqInst = &MQBase{}
	mqInst.Add(new(TestConsume))
	mqInst.Blocking().Run()
}