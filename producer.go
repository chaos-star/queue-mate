package mate

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

func (c *Client) Publish(exhangeType ExType, exchangeName string, routeKey string, body []byte, options ...interface{}) (err error) {
	var (
		ch *amqp.Channel
	)
	err = c.connection()
	if err != nil {
		return
	}
	defer c.deferReturnConnection()

	if exhangeType != "topic" && exhangeType != "direct" && exhangeType != "fanout" {
		err = errors.New("other modes are not supported")
		return
	}

	if exhangeType == c.Fanout {
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
		string(exhangeType),
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

	preview := truncateBodyForLog(body)
	if c.option.Tag != "" {
		c.log.Info(fmt.Sprintf("[MQ] [PRODUCTER] [%s] [%s] [%s] [MSG] %s", c.option.Tag, exchangeName, routeKey, preview))
	} else {
		c.log.Info(fmt.Sprintf("[MQ] [PRODUCTER] [%s] [%s] [MSG] %s", exchangeName, routeKey, preview))
	}
	return
}
