package mate

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"reflect"
	"time"
)

func (c *Client) Receive(exchangeType ExType, exchangeName string, routeKeys []string, queueName string) (err error) {
	var (
		ch       *amqp.Channel
		queue    amqp.Queue
		conn     *amqp.Connection
		messages <-chan amqp.Delivery
	)

	defer func() {
		if conn != nil {
			conn.Close()
		}
		if ch != nil {
			ch.Close()
		}
	}()

	for {
		var forever = make(chan struct{})
		if conn != nil {
			conn.Close()
		}
		if ch != nil {
			ch.Close()
		}

		//连接服务
		err = c.connection()
		if err != nil {
			return
		}
		conn = c.conn
		//无处理逻辑退出
		if c.proc == nil {
			err = errors.New("please implement the processing method")
			return
		}
		//获取MQ对象
		mcName := reflect.TypeOf(c.proc).Elem().Name()

		if c.option.Tag != "" {
			mcName = fmt.Sprintf("%s-%s", mcName, c.option.Tag)
		}

		if exchangeType != "topic" && exchangeType != "direct" && exchangeType != "fanout" {
			err = errors.New("other modes are not supported")
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}

		if exchangeType == "fanout" {
			routeKeys = []string{""}
		}

		ch, err = c.conn.Channel()
		if err != nil {
			err = errors.New(fmt.Sprintf("failed to open a channel %s", err.Error()))
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			continue
		}

		//检测消费者
		go c.Sentry(conn, ch, forever)

		err = ch.ExchangeDeclare(
			exchangeName,
			string(exchangeType),
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			err = errors.New(fmt.Sprintf("failed to declare exchange %s", err.Error()))
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}

		queue, err = ch.QueueDeclare(
			queueName,
			false,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			err = errors.New(fmt.Sprintf("failed to declare queue %s", err.Error()))
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}

		for _, routeKey := range routeKeys {
			err = ch.QueueBind(
				queue.Name,
				routeKey,
				exchangeName,
				false,
				nil,
			)
			if err != nil {
				err = errors.New(fmt.Sprintf("failed to exchange bind queue %s", err.Error()))
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
				return
			}
		}

		messages, err = ch.Consume(
			queue.Name,
			"",
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			err = errors.New(fmt.Sprintf("failed to consume %s", err.Error()))
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}

		go func() {
			var (
				num int
			)
			for msg := range messages {
				c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] [MSG] Message:%s", mcName, string(msg.Body)))
				if conn.IsClosed() || ch.IsClosed() {
					forever <- struct{}{}
					return
				}
				c.wg.Add(1)
				go func(l, n *int) {
					defer func() {
						c.wg.Done()
						if x := recover(); x != nil {
							err = msg.Ack(true)
							Exception := fmt.Sprintf("[MQ] [CONSUMER] [%s] [PANIC] Msg:%s, Exception:%#v", mcName, string(msg.Body), x)
							c.log.Error(Exception)
							//Ack掉Panic消息
							var normal bool
							normal = c.AckMessage(conn, ch, &msg, mcName, "PANIC")
							if !normal {
								forever <- struct{}{}
							}
							return
						}
					}()
					for {
						if err = c.proc.Process(msg.Body, c.option); err == nil {
							*n = 0
							var normal bool
							normal = c.AckMessage(conn, ch, &msg, mcName, "")
							if !normal {
								forever <- struct{}{}
							}
							return
						} else {
							*n++
							if *n < *l {
								c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [RETRY] [PROCESS] Message:%s, Times:%d", mcName, string(msg.Body), *n+1))
								continue
							} else {
								*n = 0
								var normal bool
								normal = c.AckMessage(conn, ch, &msg, mcName, "")
								if !normal {
									forever <- struct{}{}
								}
								return
							}
						}
					}
				}(&c.retryNum, &num)
				c.wg.Wait()
			}
		}()
		var startLog = fmt.Sprintf("[MQ] [CONSUMER] [%s] Already Started...", mcName)
		fmt.Println(startLog)
		c.log.Info(startLog)
		<-forever
		var restartLog = fmt.Sprintf("[MQ] [CONSUMER] [%s] [RESTART] Internal Restart...", mcName)
		fmt.Println(restartLog)
		c.log.Error(restartLog)
	}
	return
}

func (c *Client) Sentry(conn *amqp.Connection, ch *amqp.Channel, forever chan struct{}) {
	var gap = time.Second * 10
	var timer = time.NewTimer(gap)
	for {
		select {
		case <-timer.C:
			if conn.IsClosed() || ch.IsClosed() {
				forever <- struct{}{}
				return
			}
			timer.Reset(gap)
		}
	}
}

func (c *Client) AckMessage(conn *amqp.Connection, ch *amqp.Channel, msg *amqp.Delivery, mcName string, tag string) (connNormal bool) {
	if !conn.IsClosed() && !ch.IsClosed() {
		connNormal = true
		var num = 3
		if tag != "" {
			tag = fmt.Sprintf("[%s] ", tag)
		}
		for i := 0; i < num; i++ {
			err := msg.Ack(true)
			if err != nil {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] %s[ACK] [FAIL] Times:%d, Message: %s, Exception: %s", mcName, tag, i+1, string(msg.Body), err.Error()))
				continue
			} else {
				c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] %s[ACK] [OK] Times:%d, Message: %s", mcName, tag, i+1, string(msg.Body)))
				return
			}
		}
	}
	return
}

func (c *Client) DelayReceive(exchangeType ExType, exchangeName string, routeKeys []string, queueName string) (err error) {
	var (
		ch               *amqp.Channel
		deadExchangeName = fmt.Sprintf("delay_%s", exchangeName)
		deadQueueName    = fmt.Sprintf("delay_%s", queueName)
	)

	err = c.connection()
	if err != nil {
		return
	}
	defer c.connections.Put(c.connect)

	if c.proc == nil {
		err = errors.New("please implement the processing method")
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] Exception:%s", err.Error()))
		return
	}
	mcName := reflect.TypeOf(c.proc).Elem().Name()

	if exchangeType != "topic" && exchangeType != "direct" && exchangeType != "fanout" {
		err = errors.New("other modes are not supported")
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	if exchangeType == "fanout" {
		routeKeys = []string{""}
	}

	ch, err = c.conn.Channel()
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to open a channel %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}
	defer ch.Close()
	//业务交换机
	err = ch.ExchangeDeclare(
		exchangeName,
		string(exchangeType),
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare exchange %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	//死信交换机
	err = ch.ExchangeDeclare(
		deadExchangeName,
		string(exchangeType),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare exchange %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	var args = make(amqp.Table)
	args["x-dead-letter-exchange"] = deadExchangeName
	//业务队列
	queue, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare queue %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}
	//死信队列
	deadQueue, err := ch.QueueDeclare(
		deadQueueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare dead letter queue %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}
	//绑定死信队列
	err = ch.QueueBind(
		deadQueue.Name,
		"",
		deadExchangeName,
		false,
		nil,
	)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to dead letter exchange bind dead letter queue %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	for _, routeKey := range routeKeys {
		err = ch.QueueBind(
			queue.Name,
			routeKey,
			exchangeName,
			false,
			nil,
		)
		if err != nil {
			err = errors.New(fmt.Sprintf("failed to exchange bind queue %s", err.Error()))
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}
	}

	messages, err := ch.Consume(
		deadQueue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	var forever chan struct{}
	go func() {
		var (
			num int
		)
		for msg := range messages {
			c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] [MSG] Message:%s", mcName, string(msg.Body)))
			c.wg.Add(1)
			go func(l, n *int) {
				defer func() {
					c.wg.Done()
					if x := recover(); x != nil {
						err = msg.Ack(true)
						Exception := fmt.Sprintf("[MQ] [CONSUMER] [%s] [PANIC] Msg:%s, Exception:%#v", mcName, string(msg.Body), x)
						c.log.Error(Exception)
						fmt.Println(Exception)
					}
				}()
				for {
					if err = c.proc.Process(msg.Body, c.option); err == nil {
						err = msg.Ack(true)
						if err != nil {
							c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [ACK] Message:%s, Exception:%s", mcName, string(msg.Body), err.Error()))
						}
						*n = 0
						break
					} else {
						c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [PROCESS] Message:%s, Exception:%s, RunTimes:%d", mcName, string(msg.Body), err.Error(), *n+1))
						if *n < *l {
							*n++
							continue
						} else {
							*n = 0
							err = msg.Ack(true)
							if err != nil {
								c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [ACK] Message:%s, Exception:%s", mcName, string(msg.Body), err.Error()))
							}
							break
						}
					}
				}
			}(&c.retryNum, &num)
			c.wg.Wait()
		}
	}()

	<-forever

	return
}
