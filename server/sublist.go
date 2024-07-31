package server

import (
	"sync"
	"time"
)

const (
	TOPIC_NIL_PTP = 1 //
	TOPIC_NIL_PSB = 2
	TOPIC_KEY_PSB = 3 //map[cli_name]offset in a partition
)

//主题结构
type Topic struct {
	rmu     sync.RWMutex
	Parts   map[string]*Partition
	subList map[string]*SubScription//订阅名——订阅结构
}

//消息分区
type Partition struct {
	rmu              sync.RWMutex
	key              string
	queue            []string
	consumers_offset map[string]int
}
//订阅结构
type SubScription struct {
	rmu              sync.RWMutex        
	topic_name       string             // 订阅的主题名称
	option           int8               // 订阅的选项，用于确定订阅的类型或模式
	consumer_to_part map[string]string  // 消费者到分区的映射，键为消费者 ID，值为对应的分区键
	groups           []*Group           // 组的切片，包含所有与此订阅关联的组
}


//关闭消费者链接
func (s *SubScription) ShutdownConsumer(cli_name string) {
	s.rmu.Lock()

	switch s.option{
	case TOPIC_NIL_PTP:// point to point just one group
		s.groups[0].DownClient(cli_name)
	case TOPIC_KEY_PSB:
		for _, group := range s.groups {
			group.DownClient(cli_name)
		}
	}

	s.rmu.Unlock()

	s.Rebalance()
}

func (s *SubScription) AddConsumer(req sub) {

	switch req.option {
	case TOPIC_NIL_PTP:
		s.groups[0].AddClient(req.consumer)
	case TOPIC_KEY_PSB:
		group := NewGroup(req.topic, req.consumer)
		s.groups = append(s.groups, group)//向消费组中添加消费者
		s.consumer_to_part[req.consumer] = req.key// 将消费者和分片键的映射添加到 consumer_to_part 映射中
	}
}

func (s *SubScription) Rebalance(){

}

func NewTopic(req push) *Topic {
	topic := &Topic{
		rmu: sync.RWMutex{},
		Parts: make(map[string]*Partition),
		subList: make(map[string]*SubScription),
	}
	part := NewPartition(req)
	topic.Parts[req.key] = part

	return topic
}

// topic + "nil" + "ptp" (point to point consumer比partition为 1 : n)
// topic + key   + "psb" (pub and sub consumer比partition为 n : 1)
// topic + "nil" + "psb" (pub and sub consumer比partition为 n : n)
func (t *Topic) getStringfromSub(req sub) string {
	ret := req.topic
	if req.option == TOPIC_NIL_PTP { // 订阅发布模式
		ret = ret + "NIL" + "ptp" //point to point
	} else if req.option == TOPIC_KEY_PSB {
		ret = ret + req.key + "psb" //pub and sub
	} else if req.option == TOPIC_NIL_PSB {
		ret = ret + "NIL" + "psb"
	}
	return ret
}

//根据消费者的订阅请求将
func (t *Topic) AddSubScription(req sub) (retsub *SubScription, err error){
	ret := t.getStringfromSub(req)

	subscription, ok := t.subList[ret]
	if !ok {//如果没有该订阅则创建一个
		subscription = NewSubScription(req)
	} else {
		subscription.AddConsumer(req)
	}

	return subscription, nil
}

func NewSubScription(req sub) *SubScription {
	sub := &SubScription{
		rmu: sync.RWMutex{},
		topic_name:       req.topic,
		option:           req.option,
		consumer_to_part: make(map[string]string),
	}

	group := NewGroup(req.topic, req.consumer)
	sub.groups = append(sub.groups, group)
	sub.consumer_to_part[req.consumer] = req.key

	return sub
}

func NewPartition(req push)*Partition{
	part := &Partition{
		rmu: sync.RWMutex{},
		key: req.key,
		queue: make([]string, 40),
		consumers_offset: make(map[string]int),
	}

	part.queue = append(part.queue, req.message)

	return part
}

// Release 启动每个消费者的消息发布过程
func (p *Partition) Release(server *Server) {
	// 遍历分区中的所有消费者名称
	for consumer_name := range p.consumers_offset {
		// 获取对应消费者的指针
		server.mu.Lock() // 锁定服务器的互斥锁，确保线程安全
		con := server.consumers[consumer_name]//获取Cilnte结构体
		server.mu.Unlock() // 解锁服务器的互斥锁
		// 启动一个新的协程来处理消息发布
		go p.Pub(con)
	}
}

//发布消息
func (p *Partition)Pub(cli *Client){

	for{
		cli.mu.RLock()
		if cli.state == ALIVE {

			name := cli.name
			cli.mu.RUnlock()
			
			p.rmu.RLock()
			
			offset := p.consumers_offset[name]
			msg := p.queue[offset]
			p.rmu.RUnlock()

			ret := cli.Pub(msg)
			if ret {
				p.rmu.Lock()
				p.consumers_offset[name] = offset+1
				p.rmu.Unlock()
			}
			
		}else{
			cli.mu.RUnlock()
			time.Sleep(time.Second)
		}
	}	
}


func (t *Topic)StartRelease(server *Server){
	for _, part := range t.Parts{
		part.Release(server)   //创建新分片后，开启协程发送消息
	}
}