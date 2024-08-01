package server

//消息队列服务器的功能

import (
	"mq/kitex_gen/api/client_operations"
	"sync"

	client2 "github.com/cloudwego/kitex/client"
)

type Server struct {
	topics map[string]*Topic//服务器的主题映射，主题名——主题结构体
	// groups map[string]Group

	consumers map[string]*Client//消费者映射，消费者id——消费者对应的客户端结构

	// sublist map[string]*SubScription

	mu sync.Mutex
}

//客户端的请求结构体
//推送请求
type push struct {
	producer int64
	topic    string
	key      string
	message  string
}

//拉取请求
type pull struct {
	consumer int64
	topic    string
	key      string
}

//拉取请求的响应
type retpull struct{
	message string
}

//订阅
type sub struct{
	consumer string
	topic string
	key   string
	option int8
}

func (s *Server) make() {

	s.topics = make(map[string]*Topic)//创建一个主题映射
	// s.groups = make(map[string]Group)
	// s.sublist = make(map[string]*SubScription)
	s.consumers = make(map[string]*Client)//创建一个消费者映射
	// s.groups["default"] = Group{}

	s.mu = sync.Mutex{}

	s.StartRelease()
}

func (s* Server)StartRelease(){
	s.mu.Lock()
	for _, topic := range s.topics {
		go topic.StartRelease(s)//为每个主题创建一个协程
	}
	s.mu.Unlock()
}

// InfoHandle 处理客户端连接并管理消费者
func (s *Server) InfoHandle(ipport string) error {
    // client_operations 的 C 端
	client, err := client_operations.NewClient("client", client2.WithHostPorts(ipport))
	if err == nil { // 如果客户端创建成功
        // 锁定服务器的互斥锁，确保线程安全
		s.mu.Lock()
        // 检查服务器的消费者映射中是否已有此 IP 和端口的消费者
		consumer, ok := s.consumers[ipport]
		if !ok { // 如果消费者不存在
            // 创建新的消费者实例，并将其添加到服务器的消费者映射中
			consumer = NewClient(ipport, client)
			s.consumers[ipport] = consumer
		}
        // 启动一个新的协程来检查消费者状态
		go s.CheckConsumer(consumer)
        // 解锁互斥锁
		s.mu.Unlock()

		return nil // 返回 nil 表示没有错误
	}

	return err // 返回创建客户端时发生的错误
}

//检测消费者，如果断连，关闭链接
func (s *Server)CheckConsumer(client *Client){
	shutdown := client.CheckConsumer()
	if shutdown { //该consumer已关闭，平衡subscription
		client.mu.Lock()
		for _, subscription := range client.subList{
			subscription.ShutdownConsumer(client.name)
		}
		client.mu.Unlock()
	}
}


// subscribe 订阅
func (s *Server) SubHandle(req sub) error{
	s.mu.Lock()

	sub, err := s.topics[req.topic].AddSubScription(req)

	if err != nil{
		s.consumers[req.consumer].AddSubScription(sub)
	}

	s.mu.Unlock()
	return nil
}

func (s *Server) addMessage(topic *Topic, req push) error {
	part, ok := topic.Parts[req.key]
	if !ok {
		part = NewPartition(req)

		go part.Release(s)  //创建新分片后，开启协程发送消息

		topic.Parts[req.key] = part
	}else{
		part.rmu.Lock()
		part.queue = append(part.queue, req.message)
		part.rmu.Unlock()
	}

	return nil
}

//对于C端推送消息的处理
func (s *Server) PushHandle(req push) error {

	topic, ok := s.topics[req.topic]
	if !ok {//该节点没有主题信息
		topic = NewTopic(req)
		s.mu.Lock()
		s.topics[req.topic] = topic
		s.mu.Unlock()
	}else{//有主题信息
		s.addMessage(topic, req)
	}
	return nil
}

func (s *Server) PullHandle(req pull) ( retpull, error) {

	return retpull{}, nil
}