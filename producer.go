package mate

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (c *Client) Publish(exchangeType ExType, exchangeName string, routeKey string, body []byte, options ...interface{}) (err error) {
	var (
		ch *amqp.Channel
	)
	err = c.connection()
	if err != nil {
		return
	}
	defer c.connections.Put(c.connect)

	if exchangeType != "topic" && exchangeType != "direct" && exchangeType != "fanout" {
		err = errors.New("other modes are not supported")
		return
	}

	if exchangeType == "fanout" {
		routeKey = ""
	}

	ch, err = c.conn.Channel()
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to open a channel %s", err.Error()))
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
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var msg amqp.Publishing
	msg.ContentType = "text/plain"
	msg.Body = body
	if len(options) > 0 {
		if expire, ok := options[0].(int); ok && expire > 0 {
			msg.Expiration = fmt.Sprintf("%d", expire*1000)
		}
	}

	err = ch.PublishWithContext(ctx,
		exchangeName, // exchange
		routeKey,     // routing key
		false,        // mandatory
		false,        // immediate
		msg,
	)

	c.log.Info(fmt.Sprintf("[MQ] [PRODUCER] [%s] [%s] [MSG] %s", exchangeName, routeKey, string(body)))
	return
}
