package server

//S ——> C
import (
	"mq/kitex_gen/api"
	"mq/kitex_gen/api/client_operations"
	"context"
	"sync"
	"time"
)

const (
	ALIVE = "alive"
	DOWN = "down"
)

type Client struct{
	mu sync.RWMutex
	name string
	consumer client_operations.Client//客户端注册的操作函数
	subList []*SubScription
	// ingroups []*Group
	state string
}

//消息组结构
type Group struct{
	rmu sync.RWMutex
	topic_name string			// 组所属的主题名称
	consumers map[string]bool  	// 消费者映射，键为消费者名称，值为布尔值表示消费者是否存活
}

func NewClient(ipport string, con client_operations.Client) *Client{
	client := &Client{
		mu: sync.RWMutex{},
		name: ipport,
		consumer: con,
		state: ALIVE,
		subList: make([]*SubScription, 0),//初始化订阅列表
	}
	return client
}

func NewGroup(topic_name, cli_name string)*Group{
	group := &Group{
		rmu: sync.RWMutex{},
		topic_name: topic_name,
	}
	group.consumers[cli_name] = true
	return group
}

func (g *Group)AddClient(cli_name string){
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		g.consumers[cli_name] = true
	}
	g.rmu.Unlock()
}

func (g *Group)DownClient(cli_name string){
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		g.consumers[cli_name] = false
	}
	g.rmu.Unlock()
}

func (g *Group)DeleteClient(cli_name string){
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		delete(g.consumers, cli_name)
	}
	g.rmu.Unlock()
}

func (c *Client)CheckConsumer() bool { //心跳检测
	c.mu = sync.RWMutex{}

	for{
		resp, err := c.consumer.Pingpong(context.Background(), &api.PingPongRequest{Ping: true})//使用在rpc中注册的pingpong功能
		if err != nil || !resp.Pong {
			break
		}

		time.Sleep(time.Second)
	}
	c.mu.Lock()
	c.state = DOWN
	c.mu.Unlock()
	return true
}

//将一个订阅结构插入消费者的订阅列表中
func (c *Client)AddSubScription(sub *SubScription){
	c.mu.Lock()
	c.subList = append(c.subList, sub)
	c.mu.Unlock()
}

// publish 发布
func (c *Client)Pub(message string) bool {

	resp, err := c.consumer.Pub(context.Background(), &api.PubRequest{Meg: message})

	if err != nil || !resp.Ret {
		return false
	}

	return true
}