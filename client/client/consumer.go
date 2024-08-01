package client

import (
	"mq/kitex_gen/api"
	"mq/kitex_gen/api/server_operations"
	ser "mq/kitex_gen/api/client_operations"
	"context"
	"fmt"
	"net"

	"github.com/cloudwego/kitex/server"
)

type Consumer struct {
	Cli server_operations.Client
}

//消费者通过rpc为消息队列提供的接口，s端主动向c端发起的操作

func (c *Consumer) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	fmt.Println(req.Meg)
	return &api.PubResponse{Ret: true}, nil
}

func (c *Consumer) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	return &api.PingPongResponse{Pong: true}, nil
}

func (c *Consumer) Start_server(port string) {
	addr, _ := net.ResolveTCPAddr("tcp", port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	svr := ser.NewServer(new(Consumer), opts...)

	err := svr.Run()
	if err != nil {
		println(err.Error())
	}
}
