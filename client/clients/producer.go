package clients

import (
	"mq/kitex_gen/api"
	"mq/kitex_gen/api/server_operations"
	"context"
	"errors"
	"sync"
)

type Producer struct{
	rmu sync.RWMutex
	Cli server_operations.Client
	Name string
	Topic_Partions map[string]bool   //map[topicname+partname]bool 表示该Topic的分片是否是这个producer负责
}

type Message struct{
	Topic_name 	string
	Part_name 	string
	Msg 		string
}

func (p *Producer)Push(msg Message) error {
	index := msg.Topic_name + msg.Part_name
	p.rmu.RLock()
	_, ok := p.Topic_Partions[index]
	p.rmu.RUnlock()

	if ok{
		resp,err := p.Cli.Push(context.Background(), &api.PushRequest{
			Producer: p.Name,
			Topic: msg.Topic_name,
			Key: msg.Part_name,
			Message: msg.Msg,
		})
		if err == nil && resp.Ret {
			return nil
		}else{
			return errors.New("err != nil or resp.Ret == false")
		}
	}

	return errors.New("this topic_part do not in this producter")
}