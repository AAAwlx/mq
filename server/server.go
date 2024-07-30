package server

//消息队列服务器的功能

import (
	"mq/kitex_gen/api/client_operations"
	"sync"

	client2 "github.com/cloudwego/kitex/client"
)

type Server struct {
	topics map[string]*Topic//服务器的主题，主题名——主题结构体
	// groups map[string]Group

	consumers map[string]*Client//消费者，消费者id——消费者对应的客户端结构

	// sublist map[string]*SubScription

	mu sync.Mutex
}

type push struct {
	producer int64
	topic    string
	key      string
	message  string
}

type pull struct {
	consumer int64
	topic    string
	key      string
}

type retpull struct{
	message string
}

type sub struct{
	consumer string
	topic string
	key   string
	option int8
}

func (s *Server) make() {

	s.topics = make(map[string]*Topic)//创建一个主题
	// s.groups = make(map[string]Group)
	// s.sublist = make(map[string]*SubScription)
	s.consumers = make(map[string]*Client)//创建一个消费者
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

func (s *Server) InfoHandle(ipport string) error {

	client, err := client_operations.NewClient("client", client2.WithHostPorts(ipport))
	if err == nil {
		s.mu.Lock()
		consumer, ok := s.consumers[ipport]
		if !ok {
			consumer = NewClient(ipport, client)
			s.consumers[ipport] = consumer
		}
		go s.CheckConsumer(consumer)
		s.mu.Unlock()

		return nil
	}

	return err
}

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

func (s *Server) PushHandle(req push) error {

	topic, ok := s.topics[req.topic]
	if !ok {
		topic = NewTopic(req)
		s.mu.Lock()
		s.topics[req.topic] = topic
		s.mu.Unlock()
	}else{
		s.addMessage(topic, req)
	}
	return nil
}

func (s *Server) PullHandle(req pull) ( retpull, error) {



	return retpull{}, nil
}