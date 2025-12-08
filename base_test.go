package mate

import (
	"fmt"
	"testing"
)

var rabbit = NewRabbit(
	"172.16.110.206",
	5672,
	"guest",
	"WJbu9KAsp65Z1Yzv",
	"/super",
	30,
	1,
	30,
	nil,
)

type TestConsume struct {
}

func (t *TestConsume) RunConsume() (err error) {
	client := rabbit.NewClient()
	err = client.Use(t).Retry(3).ConsumerNum(20).Receive(client.Fanout, "ex_test_exchange", nil, "qx_test_queue")
	return
}

func (t *TestConsume) Process(body []byte) (err error) {
	fmt.Println("Test Running", string(body))
	//err = errors.New("test error")
	return
}

func TestMQBase_Run(t *testing.T) {
	//var mqInst = &MQBase{}
	//mqInst.Add(new(TestConsume))
	//mqInst.Blocking().Run()
	var over = make(chan int)
	client := rabbit.NewClient()
	go func() {
		var i int
		for i < 10000 {
			client.Publish(client.Fanout, "ex_test_exchange", "", []byte(fmt.Sprintf("message one:%d", i)))
			i++
		}
	}()

	go func() {
		var i = 10000
		for i < 20000 {
			client.Publish(client.Fanout, "ex_test_exchange", "", []byte(fmt.Sprintf("message one:%d", i)))
			i++
		}
	}()

	go func() {
		var i = 20000
		for i < 30000 {
			client.Publish(client.Fanout, "ex_test_exchange", "", []byte(fmt.Sprintf("message one:%d", i)))
			i++
		}
	}()

	go func() {
		var i = 30000
		for i < 40000 {
			client.Publish(client.Fanout, "ex_test_exchange", "", []byte(fmt.Sprintf("message one:%d", i)))
			i++
		}
	}()

	go func() {
		var i = 40000
		for i < 50000 {
			client.Publish(client.Fanout, "ex_test_exchange", "", []byte(fmt.Sprintf("message one:%d", i)))
			i++
		}
	}()
	<-over
}

func TestMQBase_DelayRun(t *testing.T) {
	//var mqInst = &MQBase{}
	//mqInst.Add(new(TestConsume))
	//mqInst.Blocking().Run()
	client := rabbit.NewClient()
	var i int
	for i < 10000 {
		client.Publish(client.Fanout, "ex_test_D_exchange", "", []byte(fmt.Sprintf("message one:%d", i)), 20)
		i++
	}

}

func TestMQBase_Consume(t *testing.T) {
	var mqInst = &MQBase{}
	mqInst.Add(new(TestConsume))
	mqInst.Blocking().Run()
}

type TestDelayConsume struct {
}

func (t *TestDelayConsume) RunConsume() (err error) {
	client := rabbit.NewClient()
	err = client.Use(t).Retry(3).ConsumerNum(20).DelayReceive(client.Fanout, "ex_test_D_exchange", nil, "qx_test_D_queue")
	return
}

func (t *TestDelayConsume) Process(body []byte) (err error) {
	fmt.Println("Test Running", string(body))
	//err = errors.New("test error")
	return
}

func TestMQBase_DelayConsume(t *testing.T) {
	var mqInst = &MQBase{}
	mqInst.Add(new(TestDelayConsume))
	mqInst.Blocking().Run()
}
