package server

import (
	"mq/kitex_gen/api/client_operations"
	"errors"
	"os"
	"sync"

	client2 "github.com/cloudwego/kitex/client"
)

var (
	name string
)

type Server struct {
	topics map[string]*Topic
	// groups map[string]Group

	consumers map[string]*Client

	// sublist map[string]*SubScription

	mu sync.RWMutex
}

type Key struct {
	Start_index int64	`json:"start_index"`
	End_index 	int64	`json:"end_index"`
	Size  		int 	`json:"size"`
}

type Message struct {
	Index 		int64 		`json:"index"`
	Topic_name 	string		`json:"topic_name"`
	Part_name 	string		`json:"part_name"`
	Msg 		[]byte		`json:"msg"`
}

type Msg struct {
	producer string
	topic    string
	key      string
	msg 	 []byte
}

type push struct {
	producer string
	topic    string
	key      string
	message  string
}

type pull struct {
	consumer string
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

type startget struct{
	cli_name  	string
    topic_name	string
    part_name	string
    offset		int64
	option 		int8
}

func (s *Server) make() {

	s.topics = make(map[string]*Topic)
	// s.groups = make(map[string]Group)
	// s.sublist = make(map[string]*SubScription)
	s.consumers = make(map[string]*Client)
	// s.groups["default"] = Group{}

	s.mu = sync.RWMutex{}

	name = GetIpport()
}

func (s *Server)CheckList(){
	str, _ := os.Getwd()
	str += name
	ret := CheckFileOrList(str)
	if !ret {
		CreateList(str)
	}
}

// InfoHandle 处理客户端信息，尝试创建客户端并将其添加到服务器的消费者列表中
func (s *Server) InfoHandle(ipport string) error {

	client, err := client_operations.NewClient("client", client2.WithHostPorts(ipport))
	if err == nil {
		s.mu.Lock()
		consumer, ok := s.consumers[ipport]
		if !ok {
			consumer = NewClient(ipport, client)
			s.consumers[ipport] = consumer
		}
		go s.CheckConsumer(consumer)//对该消费者进行监控
		s.mu.Unlock()

		return nil
	}
	
	DEBUG(dError, "Connect client %v failed\n", ipport)

	return err
}

//启动对消费者的处理
func (s *Server)StartGet(start startget) (err error) {
	/*
	新开启一个consumer关于一个topic和partition的协程来消费该partition的信息；

	查询是否有该订阅的信息；

	PTP：需要负载均衡

	PSB：不需要负载均衡
	*/
	err = nil
	switch start.option{
	case TOPIC_NIL_PTP:
		

	case TOPIC_KEY_PSB:
		s.mu.RLock()
		defer s.mu.RUnlock()

		sub_name := GetStringfromSub(start.topic_name, start.part_name, start.option)
		ret := s.consumers[start.cli_name].CheckSubscription(sub_name)
		
		if ret {  //该订阅存在
			clis := make([]*client_operations.Client, 0)
			clis = append(clis, s.consumers[start.cli_name].GetCli())
			file := s.topics[start.topic_name].GetFile(start.part_name)
			go s.consumers[start.cli_name].StartPart(start, clis, file)
		}else{    //该订阅不存在
			err = errors.New("This subscription is not exist")
		}
	default:
		err = errors.New("The option is not PTP or PSB")
	}

	return err
}

//检测消费者是否在线并做相应的处理
func (s *Server)CheckConsumer(client *Client){
	shutdown := client.CheckConsumer()//心跳检测
	if shutdown { //该consumer已关闭，平衡subscription
		client.mu.Lock()
		for _, subscription := range client.subList{
			subscription.ShutdownConsumer(client.name)
			/*
			将consumer中的Part关闭
			*/
		}
		client.mu.Unlock()
	}
}

// subscribe 订阅
func (s *Server) SubHandle(req sub) error{
	s.mu.Lock()
	top, ok := s.topics[req.topic]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub, err := top.AddSubScription(req, s.consumers[req.consumer])
	if err != nil{
		s.consumers[req.consumer].AddSubScription(sub)
	}

	s.mu.Unlock()

	return nil
}

//解除订阅
func (s *Server)UnSubHandle(req sub) error {

	s.mu.Lock()
	top, ok := s.topics[req.topic]
	if !ok {
		return errors.New("this topic not in this broker")
	}
	sub_name, err := top.ReduceSubScription(req)
	if err != nil {
		s.consumers[req.consumer].ReduceSubScription(sub_name)
	}

	s.mu.Unlock()
	return nil
}

//对客户端推送的处理
func (s *Server) PushHandle(req push) error {

	topic, ok := s.topics[req.topic]
	if !ok {
		topic = NewTopic(req)
		s.mu.Lock()
		s.topics[req.topic] = topic
		s.mu.Unlock()
	}else{
		topic.addMessage(req)
	}
	return nil
}

func (s *Server) PullHandle(req pull) ( retpull, error) {
	return retpull{}, nil
}