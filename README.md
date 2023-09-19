### QueueMate
基于Rabbit MQ的消息队列助手 

##### 1. 初始化

```go

rabbitInst := mate.NewRabbit(
    "127.0.0.1", //IP
    5672,        //端口
    "username",  //用户名
    "password",  //密码
    "/vhost",    //vhost
    30,          //最大空闲连接数 小于等于0时默认10
    3,           //单连接资源有效时长,单位:小时 小于等于0时默认1
    10,          //获取连接超时时间,单位:秒 小于等于0时默认10
    nil,         //日志对象为nil时，则为控制台输出
)

注: 日志对象必须是实现了以下方法
Info(string)
Error(string)

```

##### 2. 连接池
QueueMate 连接池以vhost为单位复用连接，以配置maxIdle为上限，如无空闲连接则等待，连接回收后
重新分配，以配置maxLifeTime为最大生命周期，如过期则释放tcp资源，重新生成资源放入连接池，循环往复。


##### 3.生产者

```go

client := rabbitInst..NewClient()
err := client.Publish(
	client.Fanout,  //消息模式 Fanout、Topic、Direct
	"x_test", //交换机名称
	"x_test_route", //路由，Fanout时此参数按空处理
	[]byte(`{"name":"jack"}`) //发布消息体
	10,  //延时时间 单位:秒 当需延时N秒后执行传此参数,立即执行可不传此参数,请正确使用此参数
	)


```

##### 4.消费者

```go

//实现下面两个方法
// 1. RunConsume()(error) 消费者启动方法
// 2. Process([]byte)(error) 消费数据逻辑方法

Example:
type ExampleConsume struct {
}


func (t *ExampleConsume) RunConsume() (err error) {
client := global.GVA_MQ.NewClient()
err = client.Use(t).Retry(3).Receive(client.Fanout, "ex_test_exchange", nil, "qx_test_queue")
return
}
//注:
//client.Use(t)//使用当前消息处理方法,即业务逻辑,不选则返回错误
//client.Retry(3)//设置重试次数，不设置则不重试
//client.Receive( //非延时消费方法 参数与延时方法参数相同
//client.DelayReceive(//延时消费方法
      
参数1: client.Fanout    //消息模式 Fanout、Topic、Direct
参数2: "ex_test_change" //交换机
参数3: nil,             //路由 Fanout 此参数无效
参数4: "qx_test_queue"  // 消费队列名
// )


func (t *ExampleConsume) Process(body []byte) (err error) {
//在此处处理业务逻辑,当err返回nil时Ack,反之记录错误日志，如设置重试次数，N次失败后Ack并记录每次错误日志
return
}
	

```


##### 5.启动消费任务

```go

baseInst := mate.MQBase{}
baseInst.Add(new(consume.MyBook)) //添加消费任务
baseInst.Blocking().Run() //以阻塞方式启动任务，如需在其他阻塞任务中启动时则无需开启阻塞模式

//注:
//	baseInst.Add([]...MQBaseConsume) //添加任务，支持批量添加
//	baseIsnt.Blocking() //阻塞模式
//	baseInst.Run() //启动任务

```