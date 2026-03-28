package mate

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
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
	dialLog     logThrottle
}

type Option struct {
	Tag  string
	Args interface{}
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
			path := mq.vhost
			if path == "" {
				path = "/"
			}
			u := &url.URL{
				Scheme: "amqp",
				User:   url.UserPassword(mq.username, mq.password),
				Host:   net.JoinHostPort(mq.host, strconv.Itoa(mq.port)),
				Path:   path,
			}
			uri := u.String()
			cfg := amqp.Config{
				Heartbeat: 30 * time.Second,
				Locale:    "en_US",
			}
			conn, err := amqp.DialConfig(uri, cfg)
			if err != nil {
				if mq.dialLog.allow("dial", mqReconnectLogInterval) {
					endpoint := net.JoinHostPort(mq.host, strconv.Itoa(mq.port))
					mq.log.Error(fmt.Sprintf("[MQ] [CONNECTION] Exception:%s, endpoint:%s vhost:%s", err.Error(), endpoint, mq.vhost))
				}
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
	Process([]byte, Option) error
}

type Client struct {
	host        string
	port        int
	username    string
	password    string
	vhost       string
	option      Option
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
	sessDiscLog logThrottle
}

func (c *Client) connection() (err error) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			d := 200 * time.Millisecond
			if attempt > 5 {
				d = 2 * time.Second
			}
			time.Sleep(d)
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		c.connect = c.connections.Get(ctx)
		cancel()

		if c.connect == nil || c.connect.Conn == nil {
			continue
		}

		ac, ok := c.connect.Conn.(*amqp.Connection)
		if !ok || ac == nil || ac.IsClosed() {
			c.connections.Discard(c.connect)
			c.connect = nil
			c.conn = nil
			continue
		}
		c.conn = ac
		return nil
	}
}

func (c *Client) Retry(num int) *Client {
	if num > 0 {
		c.retryNum = num
	}
	return c
}

func (c *Client) Use(proc MessageProcessor, option Option) *Client {
	c.proc = proc
	c.option = option
	return c
}

func (c *Client) UseOption(option Option) *Client {
	c.option = option
	return c
}
