package mate

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (c *Client) Receive(exchangeType ExType, exchangeName string, routeKeys []string, queueName string) (err error) {
	var (
		conn       *amqp.Connection
		mcName     string
		retryDelay = time.Second * 5
	)
	defer func() {
		if c.connect != nil {
			c.connections.Put(c.connect)
		}
	}()

	//获取MQ对象名称（用于日志）
	if c.proc != nil {
		mcType := reflect.TypeOf(c.proc)
		if mcType.Kind() == reflect.Ptr {
			mcName = mcType.Elem().Name()
		} else {
			mcName = mcType.Name()
		}
	} else {
		mcName = "Unknown"
	}

	//模式验证（配置错误，应该退出）
	if exchangeType != "topic" && exchangeType != "direct" && exchangeType != "fanout" {
		err = errors.New("other modes are not supported")
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	for {
		//连接服务
		err = c.connection()
		if err != nil {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [CONNECTION] Failed to connect, retry after %v: %s", mcName, retryDelay, err.Error()))
			time.Sleep(retryDelay)
			continue
		}
		conn = c.conn

		//无处理逻辑退出（配置错误，应该退出）
		if c.proc == nil {
			err = errors.New("please implement the processing method")
			return
		}

		if exchangeType == "fanout" {
			routeKeys = []string{""}
		}

		//open channel 如果失败重连
		if c.consumerNum > 0 {
			var consumerWg = &sync.WaitGroup{}
			consumerWg.Add(c.consumerNum)
			for i := 0; i < c.consumerNum; i++ {
				go c.goConsumer(i, consumerWg, conn, exchangeType, exchangeName, queueName, routeKeys, mcName)
			}
			c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] Running...", mcName))
			consumerWg.Wait()
		} else {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] ConsumerNum is 0, retry after %v", mcName, retryDelay))
			time.Sleep(retryDelay)
			continue
		}

		var restartLog = fmt.Sprintf("[MQ] [CONSUMER] [%s] [RESTART] Internal Restart...", mcName)
		c.log.Info(restartLog)
		time.Sleep(retryDelay)
	}
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
			err := msg.Ack(false)
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
		mcName     string
		retryDelay = time.Second * 5
	)
	defer func() {
		if c.connect != nil {
			c.connections.Put(c.connect)
		}
	}()

	//获取MQ对象名称（用于日志）
	if c.proc != nil {
		mcType := reflect.TypeOf(c.proc)
		if mcType.Kind() == reflect.Ptr {
			mcName = mcType.Elem().Name()
		} else {
			mcName = mcType.Name()
		}
	} else {
		mcName = "Unknown"
	}

	//模式验证（配置错误，应该退出）
	if exchangeType != "topic" && exchangeType != "direct" && exchangeType != "fanout" {
		err = errors.New("other modes are not supported")
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	for {
		err = c.connection()
		if err != nil {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] [CONNECTION] Failed to connect, retry after %v: %s", mcName, retryDelay, err.Error()))
			time.Sleep(retryDelay)
			continue
		}

		//无处理逻辑退出（配置错误，应该退出）
		if c.proc == nil {
			err = errors.New("please implement the processing method")
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}

		if exchangeType == "fanout" {
			routeKeys = []string{""}
		}

		if c.consumerNum > 0 {
			var consumerWg = &sync.WaitGroup{}
			consumerWg.Add(c.consumerNum)
			for i := 0; i < c.consumerNum; i++ {
				go c.goDelayConsumer(i, consumerWg, c.conn, exchangeType, exchangeName, queueName, routeKeys, mcName)
			}
			consumerWg.Wait()
		} else {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] ConsumerNum is 0, retry after %v", mcName, retryDelay))
			time.Sleep(retryDelay)
			continue
		}

		var restartLog = fmt.Sprintf("[MQ] [CONSUMER] [%s] [RESTART] Internal Restart...", mcName)
		c.log.Info(restartLog)
		time.Sleep(retryDelay)
	}

}

func (c *Client) goConsumer(id int, wg *sync.WaitGroup, conn *amqp.Connection, exchangeType ExType, exchangeName string, queueName string, routes []string, mcName string) {
	var (
		err        error
		n          int
		messages   <-chan amqp.Delivery
		msgItem    *amqp.Delivery
		ch         *amqp.Channel
		queue      amqp.Queue
		retryDelay = time.Second * 3
		maxRetries = 5
		retryCount = 0
	)
	defer wg.Done()
	defer func() {
		if ch != nil && !ch.IsClosed() {
			if closeErr := ch.Close(); closeErr != nil {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [CLOSE] Channel close error: %s", mcName, id, closeErr.Error()))
			}
		}
		if xy := recover(); xy != nil {
			var msgBody string
			if msgItem != nil {
				msgBody = string(msgItem.Body)
			} else {
				msgBody = "unknown"
			}
			Exception := fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [PANIC] Msg:%s, Exception:%#v", mcName, id, msgBody, xy)
			c.log.Error(Exception)
			//Ack掉Panic消息
			//c.AckMessage(conn, ch, msgItem, mcName, "PANIC")
		}
	}()

	//初始化重试循环
	for retryCount < maxRetries {
		//检查连接状态
		if conn.IsClosed() {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Connection is closed, retry after %v (attempt %d/%d)", mcName, id, retryDelay, retryCount+1, maxRetries))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//开启channel
		ch, err = c.conn.Channel()
		if err != nil {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to open channel, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//定义exchange
		err = ch.ExchangeDeclare(exchangeName, string(exchangeType), true, false, false, false, nil)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to declare exchange, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//定义队列
		queue, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to declare queue, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//绑定队列
		var bindError bool
		for _, routeKey := range routes {
			err = ch.QueueBind(queue.Name, routeKey, exchangeName, false, nil)
			if err != nil {
				if ch != nil && !ch.IsClosed() {
					ch.Close()
				}
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to bind queue, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
				bindError = true
				break
			}
		}
		if bindError {
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//开启消费者
		messages, err = ch.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to consume, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//初始化成功，重置重试计数
		retryCount = 0
		break
	}

	//如果重试失败，记录错误并退出
	if retryCount >= maxRetries {
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to initialize after %d attempts, exiting", mcName, id, maxRetries))
		return
	}

	for msg := range messages {
		msgItem = &msg
		c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [MSG] Message:%s", mcName, id, string(msg.Body)))
		if conn.IsClosed() || ch.IsClosed() {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Connection or channel closed, exiting consumer", mcName, id))
			return
		}
		for {
			if err = c.proc.Process(msg.Body); err == nil {
				n = 0
				c.AckMessage(conn, ch, &msg, mcName, "")
				break
			} else {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [PROCESS] [EXCEPTION] Message:%s,  Err:%s", mcName, id, string(msg.Body), err.Error()))
				n++
				if c.retryNum > 0 && n < c.retryNum {
					c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [RETRY] [PROCESS] Message:%s, Times:%d", mcName, id, string(msg.Body), n+1))
					continue
				} else {
					n = 0
					c.AckMessage(conn, ch, &msg, mcName, "")
					break
				}
			}
		}
	}

	//消息通道关闭，记录日志
	c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Message channel closed, exiting consumer", mcName, id))
}

func (c *Client) goDelayConsumer(id int, wg *sync.WaitGroup, conn *amqp.Connection, exchangeType ExType, exchangeName string, queueName string, routes []string, mcName string) {
	var (
		err              error
		n                int
		messages         <-chan amqp.Delivery
		msgItem          *amqp.Delivery
		ch               *amqp.Channel
		deadCh           *amqp.Channel
		queue            amqp.Queue
		deadQueue        amqp.Queue
		deadExchangeName = fmt.Sprintf("delay_%s", exchangeName)
		deadQueueName    = fmt.Sprintf("delay_%s", queueName)
		retryDelay       = time.Second * 3
		maxRetries       = 5
		retryCount       = 0
	)
	defer wg.Done()
	defer func() {
		if ch != nil && !ch.IsClosed() {
			if closeErr := ch.Close(); closeErr != nil {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [CLOSE] Channel close error: %s", mcName, id, closeErr.Error()))
			}
		}
		if deadCh != nil && !deadCh.IsClosed() {
			if closeErr := deadCh.Close(); closeErr != nil {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [CLOSE] DeadChannel close error: %s", mcName, id, closeErr.Error()))
			}
		}
		if xy := recover(); xy != nil {
			var msgBody string
			if msgItem != nil {
				msgBody = string(msgItem.Body)
			} else {
				msgBody = "unknown"
			}
			Exception := fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [PANIC] Msg:%s, Exception:%#v", mcName, id, msgBody, xy)
			c.log.Error(Exception)
			//Ack掉Panic消息
			//c.AckMessage(conn, ch, msgItem, mcName, "PANIC")
		}
	}()

	//初始化重试循环
	for retryCount < maxRetries {
		//检查连接状态
		if conn.IsClosed() {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Connection is closed, retry after %v (attempt %d/%d)", mcName, id, retryDelay, retryCount+1, maxRetries))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//死信通道
		ch, err = c.conn.Channel()
		if err != nil {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to open channel, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//死信交换机
		err = ch.ExchangeDeclare(exchangeName, string(exchangeType), true, false, false, false, nil)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to declare exchange, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//死信队列
		var args = make(amqp.Table)
		args["x-dead-letter-exchange"] = deadExchangeName
		queue, err = ch.QueueDeclare(queueName, false, false, false, false, args)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to declare queue, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//绑定死信队列
		var bindError bool
		for _, routeKey := range routes {
			err = ch.QueueBind(queue.Name, routeKey, exchangeName, false, nil)
			if err != nil {
				if ch != nil && !ch.IsClosed() {
					ch.Close()
				}
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to bind queue, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
				bindError = true
				break
			}
		}
		if bindError {
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//业务通道
		deadCh, err = c.conn.Channel()
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to open dead channel, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//业务交换机
		err = deadCh.ExchangeDeclare(deadExchangeName, string(exchangeType), true, false, false, false, nil)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			if deadCh != nil && !deadCh.IsClosed() {
				deadCh.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to declare dead exchange, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//死信队列
		deadQueue, err = deadCh.QueueDeclare(deadQueueName, false, false, false, false, nil)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			if deadCh != nil && !deadCh.IsClosed() {
				deadCh.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to declare dead letter queue, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//绑定死信队列
		err = deadCh.QueueBind(deadQueue.Name, "", deadExchangeName, false, nil)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			if deadCh != nil && !deadCh.IsClosed() {
				deadCh.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to bind dead letter queue, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		var deadBindError bool
		for _, routeKey := range routes {
			err = deadCh.QueueBind(deadQueue.Name, routeKey, deadExchangeName, false, nil)
			if err != nil {
				if ch != nil && !ch.IsClosed() {
					ch.Close()
				}
				if deadCh != nil && !deadCh.IsClosed() {
					deadCh.Close()
				}
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to bind dead queue with route, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
				deadBindError = true
				break
			}
		}
		if deadBindError {
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//开启消费者
		messages, err = deadCh.Consume(deadQueueName, "", false, false, false, false, nil)
		if err != nil {
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
			if deadCh != nil && !deadCh.IsClosed() {
				deadCh.Close()
			}
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to consume, retry after %v (attempt %d/%d): %s", mcName, id, retryDelay, retryCount+1, maxRetries, err.Error()))
			time.Sleep(retryDelay)
			retryCount++
			continue
		}

		//初始化成功，重置重试计数
		retryCount = 0
		break
	}

	//如果重试失败，记录错误并退出
	if retryCount >= maxRetries {
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Failed to initialize after %d attempts, exiting", mcName, id, maxRetries))
		return
	}

	for msg := range messages {
		msgItem = &msg
		c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [MSG] Message:%s", mcName, id, string(msg.Body)))
		if conn.IsClosed() || deadCh.IsClosed() {
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Connection or channel closed, exiting consumer", mcName, id))
			return
		}
		for {
			if err = c.proc.Process(msg.Body); err == nil {
				n = 0
				c.AckMessage(conn, deadCh, &msg, mcName, "")
				break
			} else {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [PROCESS] [EXCEPTION] Message:%s,  Err:%s", mcName, id, string(msg.Body), err.Error()))
				n++
				if c.retryNum > 0 && n < c.retryNum {
					c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [RETRY] [PROCESS] Message:%s, Times:%d", mcName, id, string(msg.Body), n+1))
					continue
				} else {
					n = 0
					c.AckMessage(conn, deadCh, &msg, mcName, "")
					break
				}
			}
		}
	}

	//消息通道关闭，记录日志
	c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] Message channel closed, exiting consumer", mcName, id))
}
