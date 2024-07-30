package server

import (
	"mq/kitex_gen/api"
	"mq/kitex_gen/api/server_operations"
	"context"
	"fmt"
	"sync"
	"github.com/cloudwego/kitex/server"
)

type RPCServer struct {
	// me int64
	logging struct {
		sync.RWMutex//日志
		logger      Logger
		trace       int32
		debug       int32
		traceSysAcc int32
	}
	server Server
}

func (s *RPCServer) Start(opts []server.Option) error {
	svr := server_operations.NewServer(s, opts...)//创建一个rpc的服务端

	s.server.make()//初始化
	
	go func() {//启动一个rpc的服务线程
		err := svr.Run()
		if err != nil {
			fmt.Println(err.Error())
		}
	}()

	return nil
}

func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (resp *api.PushResponse, err error) {
	fmt.Println(req)
	
	err = s.server.PushHandle(push{
		producer: req.Producer,
		topic: req.Topic,
		key: req.Key,
		message: req.Message,
	})
	if err == nil {
		return &api.PushResponse{Ret: true,}, nil
	}
	
	return &api.PushResponse{Ret: false,}, err
}

func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (resp *api.PullResponse, err error) {
	
	ret, err := s.server.PullHandle(pull{
		consumer: req.Consumer,
		topic: req.Topic,
		key: req.Key,
	})

	if err == nil {
		return &api.PullResponse{Message: ret.message}, nil
	}

	return &api.PullResponse{Message: "111"}, nil
}

func (s *RPCServer) Info(ctx context.Context, req *api.InfoRequest) (resp *api.InfoResponse, err error) {
	//get client_server's ip and port

	err = s.server.InfoHandle(req.IpPort)
	if err == nil {
		return &api.InfoResponse{Ret: true}, nil;
	}
	
	return &api.InfoResponse{Ret: false}, err;
}

func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (resp *api.SubResponse, err error) {

	err = s.server.SubHandle(sub{
		consumer: req.Consumer,
		topic: req.Topic,
		key: req.Key,
		option: req.Option,
	})

	if err == nil{
		return &api.SubResponse{Ret: true}, nil
	}

	return &api.SubResponse{Ret: false}, err
}