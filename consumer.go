package mate

import (
	"errors"
	"fmt"
	"reflect"

	amqp "github.com/rabbitmq/amqp091-go"
)

const maxLogBody = 512

func truncateBodyForLog(b []byte) string {
	if len(b) <= maxLogBody {
		return string(b)
	}
	return string(b[:maxLogBody]) + "...(truncated)"
}

func (c *Client) runDeliveryLoop(messages <-chan amqp.Delivery, mcName string) {
	var num int
	for msg := range messages {
		preview := truncateBodyForLog(msg.Body)
		c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] [MSG] Message:%s", mcName, preview))
		c.wg.Add(1)
		go func(l, n *int, m amqp.Delivery) {
			defer func() {
				c.wg.Done()
				if x := recover(); x != nil {
					_ = m.Ack(true)
					exception := fmt.Sprintf("[MQ] [CONSUMER] [%s] [PANIC] Msg:%s, Exception:%#v", mcName, truncateBodyForLog(m.Body), x)
					c.log.Error(exception)
				}
			}()
			for {
				if err := c.proc.Process(m.Body, c.option); err == nil {
					if ackErr := m.Ack(true); ackErr != nil {
						c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [ACK] Message:%s, Exception:%s", mcName, truncateBodyForLog(m.Body), ackErr.Error()))
					}
					*n = 0
					break
				} else {
					c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [PROCESS] Message:%s, Exception:%s, RunTimes:%d", mcName, truncateBodyForLog(m.Body), err.Error(), *n+1))
					if *n < *l {
						*n++
						continue
					} else {
						*n = 0
						if ackErr := m.Ack(true); ackErr != nil {
							c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [ACK] Message:%s, Exception:%s", mcName, truncateBodyForLog(m.Body), ackErr.Error()))
						}
						break
					}
				}
			}
		}(&c.retryNum, &num, msg)
		c.wg.Wait()
	}
}

func (c *Client) waitConsumeSession(mcName string, conn *amqp.Connection, ch *amqp.Channel, messages <-chan amqp.Delivery) error {
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	chClose := ch.NotifyClose(make(chan *amqp.Error, 1))
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.runDeliveryLoop(messages, mcName)
	}()

	select {
	case <-done:
		if c.sessDiscLog.allow(mcName, mqReconnectLogInterval) {
			c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] deliveries ended, will reconnect", mcName))
		}
	case amqpErr := <-connClose:
		if !c.sessDiscLog.allow(mcName, mqReconnectLogInterval) {
			break
		}
		if amqpErr != nil {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] connection closed: %v", mcName, amqpErr))
		} else {
			c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] connection closed", mcName))
		}
	case amqpErr := <-chClose:
		if !c.sessDiscLog.allow(mcName, mqReconnectLogInterval) {
			break
		}
		if amqpErr != nil {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] channel closed: %v", mcName, amqpErr))
		} else {
			c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] channel closed", mcName))
		}
	}

	return errors.New("mq consumer session ended")
}

func (c *Client) deferReturnConnection() {
	if c.connect == nil {
		return
	}
	if c.conn != nil && !c.conn.IsClosed() {
		c.connections.Put(c.connect)
	} else {
		c.connections.Discard(c.connect)
	}
}

func (c *Client) Receive(exchangeType ExType, exchangeName string, routeKeys []string, queueName string) (err error) {
	var ch *amqp.Channel

	err = c.connection()
	if err != nil {
		return
	}
	defer c.deferReturnConnection()

	if c.proc == nil {
		err = errors.New("please implement the processing method")
		return
	}
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
		return
	}
	defer ch.Close()

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

	queue, err := ch.QueueDeclare(
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

	err = ch.Qos(1, 0, false)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to set qos %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	messages, err := ch.Consume(
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

	return c.waitConsumeSession(mcName, c.conn, ch, messages)
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
	defer c.deferReturnConnection()

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

	err = ch.Qos(1, 0, false)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to set qos %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
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
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to consume %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	return c.waitConsumeSession(mcName, c.conn, ch, messages)
}
