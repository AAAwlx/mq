package server

import (
	"mq/kitex_gen/api"
	"mq/kitex_gen/api/client_operations"
	"context"
	"errors"
	"os"
	"sync"
	"time"
)

const (
	ALIVE = "alive"
	DOWN = "down"
)

type Config struct{

}

type Client struct{
	mu sync.RWMutex
	name string
	state string
	consumer client_operations.Client
	parts map[string]Part

	// ingroups []*Group
	subList map[string]*SubScription  // 若这个consumer关闭则遍历这些订阅并修改
}


func NewClient(ipport string, con client_operations.Client) *Client{
	client := &Client{
		mu: sync.RWMutex{},
		name: ipport,
		consumer: con,
		state: ALIVE,
		parts: make(map[string]Part),
		subList: make(map[string]*SubScription),
	}
	return client
}


func (c *Client)CheckConsumer() bool { //心跳检测
	c.mu = sync.RWMutex{}

	for{
		resp, err := c.consumer.Pingpong(context.Background(), &api.PingPongRequest{Ping: true})
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

func (c *Client)CheckSubscription(sub_name string) bool {
	c.mu.RLock()
	_, ok := c.subList[sub_name]
	c.mu.Unlock()

	return ok
}

func (c *Client)AddSubScription(sub *SubScription){
	c.mu.Lock()
	c.subList[sub.name] = sub
	c.mu.Unlock()
}

func (c *Client)ReduceSubScription(name string){
	c.mu.Lock()
	delete(c.subList, name)
	c.mu.Unlock()
}

func (c *Client)GetCli() *client_operations.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &c.consumer
}

//启动一个对分片处理
func (c *Client)StartPart(start startget, clis []*client_operations.Client, file *File){
	c.mu.Lock()
	part, ok := c.parts[start.part_name]
	if !ok {
		part = NewPart(start, clis, file)
		c.parts[start.part_name] = part
	}
	go part.Start()
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

type Part struct{
	mu sync.RWMutex
	topic_name 	string
	part_name 	string
	option 		int8
	clis 		[]*client_operations.Client
	
	state 		string
	fd 			os.File
	file 		*File

	offset 		int64
	buffer 		[]Message

	part_had	chan int
	buf_do 		map[int]bool
	buf_done	map[int]bool
}

//创建一个消息分片
func NewPart(start startget, clis []*client_operations.Client, file *File) Part {
	return Part{
		mu : sync.RWMutex{},
		topic_name: start.topic_name,
		part_name: start.part_name,
		option: start.option,
		offset: start.offset,
		buffer: make([]Message, 50),
		file: file,
		clis: clis,
		state: ALIVE,

		part_had: make(chan int),
		buf_do: make(map[int]bool),
		buf_done: make(map[int]bool),
	}
}

//分片对于消费者

// 它负责循环读取数据并将数据发送给消费者。
func (p *Part) Start() {

    /*
    打开文件
    */
    p.fd = *p.file.OpenFile() // 使用分片的文件对象来打开文件，获取文件描述符

    for {
        // 使用读锁锁定状态，检查分片的状态
        p.mu.RLock()
        if p.state == DOWN { // 如果状态为 DOWN，则退出循环
            break
        }
        p.mu.RUnlock()
        
        /*
        循环 clis，按块发送信息，例如两个 consumer 消费这个 part，
        我们启动两个 goroutine 去发送。使用条件变量或 select 管道来判断是否成功，
        成功则继续，失败或超时则进行其他处理。
        */
        // 如果缓冲区中的数据少于 3 * VERTUAL_10，则可以继续处理
        if len(p.buffer) < 3 * VERTUAL_10 {
            // 处理不足的数据情况
        }

        // 使用读锁锁定以确保在发送数据时的线程安全
        p.mu.RLock()
        // 遍历所有消费者，逐个发送数据
        for _, cli := range p.clis {

            var msg []Message // 创建一个消息切片，用于存储要发送的数据
            
            // 检查数据偏移量是否对齐到 VERTUAL_10 的整数倍
            if p.offset % VERTUAL_10 != 0 {
                // 如果偏移量不对齐，处理偏移量不对齐的情况
            }
            // 将缓冲区中的数据按块添加到消息中
            for i := 0; i < VERTUAL_10; i++ {
                msg = append(msg, p.buffer[i])
            }
        
            // 启动一个新的 goroutine 来发布消息
            go p.Pub()
        }
        p.mu.RUnlock()
    }
}


func (p *Part)Pub(){

}

func (p *Part)ClosePart(){
	p.mu.Lock()
	defer p.mu.Unlock()

	p.state = DOWN
}

type Group struct{
	rmu sync.RWMutex
	topic_name string
	consumers map[string]bool // map[client'name]alive
}

//创建新的消费组
func NewGroup(topic_name, cli_name string)*Group{
	group := &Group{
		rmu: sync.RWMutex{},
		topic_name: topic_name,
	}
	group.consumers[cli_name] = true
	return group
}

//恢复客户端
func (g *Group)RecoverClient(cli_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()

	_, ok := g.consumers[cli_name]
	if ok {
		if g.consumers[cli_name] {
			return errors.New("This client is alive before")
		}else{
			g.consumers[cli_name] = true
			return nil
		}
	}else{
		return errors.New("Do not have this client")
	}	
}

//向组内添加消费者
func (g *Group)AddClient(cli_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	_, ok := g.consumers[cli_name]
	if ok {
		return errors.New("this client has in this group")
	}else{
		g.consumers[cli_name] = true
		return nil
	}
}

//消费者下线
func (g *Group)DownClient(cli_name string){
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		g.consumers[cli_name] = false
	}
	g.rmu.Unlock()
}

//从组内移除消费者
func (g *Group)DeleteClient(cli_name string){
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		delete(g.consumers, cli_name)
	}
	g.rmu.Unlock()
}
