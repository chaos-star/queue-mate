package mate

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Connection struct {
	Conn   interface{}
	expire time.Time
}

func (c *Connection) expired() (flag bool) {
	if c.expire.Sub(time.Now()) <= 0 {
		flag = true
	}
	return
}

type ConnectionPool struct {
	log             Logger
	lock            sync.Mutex
	numOpen         int
	numIdle         int
	waitConnections []chan *Connection
	idleConnections []*Connection
	MaxLifeTime     time.Duration
	MaxIdle         int
	NewFunc         func() interface{}
	Close           func(interface{}) error
}

func (cp *ConnectionPool) Get(ctx context.Context) (conn *Connection) {
	cp.lock.Lock()
	var flag int

	defer func() {
		if flag == 0 {
			cp.lock.Unlock()
		}
	}()
	if len(cp.idleConnections) > 0 {
		var i int
		for {
			//无空闲连接则跳出新建
			if len(cp.idleConnections) <= 0 {
				break
			}
			conn = cp.idleConnections[0]
			if conn.expired() {
				if conn.Conn != nil {
					if err := cp.Close(conn.Conn); err != nil {
						cp.log.Error(fmt.Sprintf("[MQ] [CONNECTION] [CLOSE] Index:%d, Exception:%s", i, err.Error()))
						//关闭3次仍未关闭则属于僵尸连接，忽略此链接，丢出连接池
						if i < 3 {
							i++
							continue
						}
					}
				}

				//重新获取连接
				i = 0
				cp.idleConnections = cp.idleConnections[1:]
				cp.numIdle--
				if len(cp.idleConnections) <= 0 {
					//无闲置连接则跳出，寻求其他方式获取链接
					break
				}
				continue
			} else {
				//拿到闲置连接
				cp.idleConnections = cp.idleConnections[1:]
				cp.numOpen++
				cp.numIdle--
				return
			}
		}
	}

	//如果还有剩余坐席
	if cp.numOpen+cp.numIdle < cp.MaxIdle {
		temp := Connection{}
		temp.Conn = cp.NewFunc()
		temp.expire = time.Now().Add(cp.MaxLifeTime)
		conn = &temp
		cp.numOpen++
		return
	}
	//如果无空闲 无空席则等待
	flag = 1
	req := make(chan *Connection)
	cp.waitConnections = append(cp.waitConnections, req)
	cp.lock.Unlock()
	select {
	case <-ctx.Done():
		cp.lock.Lock()
		for index, item := range cp.waitConnections {
			if item == req {
				cp.waitConnections = append(cp.waitConnections[:index], cp.waitConnections[index+1:]...)
				break
			}
		}
		//如果在此瞬间返回了连接则销毁或者回收
		select {
		default:
		case xConn := <-req:
			if xConn.expired() {
				err := cp.Close(xConn.Conn)
				if err != nil {
					cp.log.Info(fmt.Sprintf("[MQ] [CONNECTION] Stop Or Timeout Close Exception:%s", err.Error()))
				}
				cp.numOpen--
			} else {
				cp.idleConnections = append(cp.idleConnections, xConn)
			}
		}
		close(req)
		cp.lock.Unlock()
	case xConn := <-req:
		if xConn.expired() {
			err := cp.Close(xConn.Conn)
			if err != nil {
				cp.log.Info(fmt.Sprintf("[MQ] [CONNECTION] Wait Close Exception:%s", err.Error()))
			}
			temp := Connection{}
			temp.Conn = cp.NewFunc()
			temp.expire = time.Now().Add(cp.MaxLifeTime)
			conn = &temp
		} else {
			conn = xConn
		}
		close(req)
		return
	}
	return
}
func (cp *ConnectionPool) Put(conn *Connection) {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	if len(cp.waitConnections) > 0 {
		req := cp.waitConnections[0]
		if req != nil {
			req <- conn
			cp.waitConnections = cp.waitConnections[1:]
		}
		return
	}
	cp.numOpen--
	if cp.numIdle+cp.numOpen >= cp.MaxIdle {
		err := cp.Close(conn.Conn)
		if err != nil {
			cp.log.Info(fmt.Sprintf("[MQ] [CONNECTION] Put Close Exception:%s", err.Error()))
		}
		return
	}
	cp.idleConnections = append(cp.idleConnections, conn)
	cp.numIdle++
	return
}
