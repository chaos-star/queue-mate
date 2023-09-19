package mate

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
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

func NewRabbit(host string, port int, username, password, vhost string, maxIdle int, maxLifeTime time.Duration, timeout time.Duration, logger Logger) *Rabbit {
	if logger == nil {
		logger = new(ConsoleOutput)
	}
	if maxIdle <= 0 {
		maxIdle = 10
	}
	if maxLifeTime <= 0 {
		maxLifeTime = time.Duration(1)
	}
	if timeout <= 0 {
		timeout = time.Duration(10)
	}
	var mq = &Rabbit{
		host:        host,
		port:        port,
		username:    username,
		password:    password,
		vhost:       vhost,
		log:         logger,
		maxIdle:     maxIdle,
		maxLifeTime: maxLifeTime * time.Hour,
		timeout:     timeout * time.Second,
	}

	mq.connections = &ConnectionPool{
		log:         logger,
		MaxIdle:     mq.maxIdle,
		MaxLifeTime: mq.maxLifeTime,
		Close: func(conn interface{}) error {
			return conn.(*amqp.Connection).Close()
		},
		NewFunc: func() interface{} {
			config := fmt.Sprintf("amqp://%s:%s@%s:%d%s", mq.username, mq.password, mq.host, mq.port, mq.vhost)
			conn, err := amqp.Dial(config)
			if err != nil {
				mq.log.Error(fmt.Sprintf("[MQ] [CONNECTION] Exception:%s, conf:%s", err.Error(), config))
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
	proc        MessageProcessor
	log         Logger
}

func (c *Client) connection() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	c.connect = c.connections.Get(ctx)
	if c.connect == nil || c.connect.Conn == nil {
		err = errors.New("[MQ] [CONNECTION] Exception:Timeout")
		return
	}

	c.conn = c.connect.Conn.(*amqp.Connection)
	if c.conn == nil {
		message := "[MQ] [CONNECTION] Exception:Invalid Tcp resource"
		c.log.Error(message)
		err = errors.New(message)
	}

	return
}

func (c *Client) Retry(num int) *Client {
	if num > 0 {
		c.retryNum = num
	}
	return c
}

func (c *Client) Use(proc MessageProcessor) *Client {
	c.proc = proc
	return c
}
