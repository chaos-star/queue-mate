package mate

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExType string

type Rabbit struct {
	host        string
	port        int
	username    string
	password    string
	vhost       string
	maxIdle     int
	maxLifeTime time.Duration
	timeout     time.Duration
	log         Logger
	connections *ConnectionPool
}

func NewRabbit(host string, port int, username, password, vhost string, maxIdle int, maxLifeTimeHours int, timeoutSeconds int, logger Logger) *Rabbit {
	if logger == nil {
		logger = new(ConsoleOutput)
	}
	if maxIdle <= 0 {
		maxIdle = 10
	}
	if maxLifeTimeHours <= 0 {
		maxLifeTimeHours = 1
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = 10
	}
	var mq = &Rabbit{
		host:        host,
		port:        port,
		username:    username,
		password:    password,
		vhost:       vhost,
		log:         logger,
		maxIdle:     maxIdle,
		maxLifeTime: time.Duration(maxLifeTimeHours) * time.Hour,
		timeout:     time.Duration(timeoutSeconds) * time.Second,
	}

	mq.connections = &ConnectionPool{
		log:         logger,
		MaxIdle:     mq.maxIdle,
		MaxLifeTime: mq.maxLifeTime,
		Close: func(conn interface{}) error {
			if amqpConn, ok := conn.(*amqp.Connection); ok {
				return amqpConn.Close()
			}
			return fmt.Errorf("invalid connection type")
		},
		NewFunc: func() interface{} {
			config := fmt.Sprintf("amqp://%s:%s@%s:%d%s", mq.username, mq.password, mq.host, mq.port, mq.vhost)
			conn, err := amqp.Dial(config)
			if err != nil {
				// 避免在日志中暴露密码
				safeConfig := fmt.Sprintf("amqp://%s:***@%s:%d%s", mq.username, mq.host, mq.port, mq.vhost)
				mq.log.Error(fmt.Sprintf("[MQ] [CONNECTION] Exception:%s, conf:%s", err.Error(), safeConfig))
				return nil
			}
			return conn
		},
	}

	return mq
}

func (r Rabbit) NewClient() *Client {
	return &Client{
		log:         r.log,
		timeout:     r.timeout,
		connections: r.connections,
		wg:          &sync.WaitGroup{},
		Topic:       "topic",
		Direct:      "direct",
		Fanout:      "fanout",
		consumerNum: 1,
	}
}

type MessageProcessor interface {
	Process([]byte) error
}

type Client struct {
	host        string
	port        int
	username    string
	password    string
	vhost       string
	Topic       ExType
	Direct      ExType
	Fanout      ExType
	connect     *Connection
	connections *ConnectionPool
	conn        *amqp.Connection
	wg          *sync.WaitGroup
	timeout     time.Duration
	retryNum    int
	consumerNum int
	proc        MessageProcessor
	log         Logger
}

func (c *Client) connection() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	for {
		c.connect = c.connections.Get(ctx)
		if c.connect == nil || c.connect.Conn == nil {
			//c.log.Info("[MQ] [CONNECTION] Invalid Tcp Resource Retry")
			c.connections.Reset()
			continue
		}

		var ok bool
		c.conn, ok = c.connect.Conn.(*amqp.Connection)
		if !ok || c.conn == nil {
			//c.log.Info("[MQ] [CONNECTION] Invalid Connection Type Retry")
			c.connections.Reset()
			continue
		}

		if c.conn.IsClosed() {
			//c.log.Info("[MQ] [CONNECTION] Closed Tcp Resource Retry")
			c.connections.Reset()
			continue
		}
		break
	}

	return
}

func (c *Client) Retry(num int) *Client {
	if num > 0 {
		c.retryNum = num
	}
	return c
}

func (c *Client) ConsumerNum(num int) *Client {
	if num > 0 {
		c.consumerNum = num
	}
	return c
}

func (c *Client) Use(proc MessageProcessor) *Client {
	c.proc = proc
	return c
}
