package main

import (
	"mq/kitex_gen/api"
	ser "mq/kitex_gen/api/client_operations"
	"mq/kitex_gen/api/server_operations"
	"context"
	"fmt"
	"net"
	"time"
	client3 "mq/client/client"
	client2 "github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
)

type Server struct{

}

//消费者通过rpc为消息队列提供的接口，s端主动向c端发起的操作
func (s *Server)Pub(ctx context.Context, req *api.PubRequest)(resp *api.PubResponse, err error){
	fmt.Println(req.Meg)
	return &api.PubResponse{Ret: true}, nil
}

func (s *Server)Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	return &api.PingPongResponse{Pong: true}, nil
}

func start_server(port string) {
	// 解析端口字符串，生成 TCP 地址对象
	addr, _ := net.ResolveTCPAddr("tcp", port)

	// 定义一个空的 server.Option 切片
	var opts []server.Option

	// 将服务地址选项添加到 opts 切片中，选择编码器
	opts = append(opts, server.WithServiceAddr(addr))

	// 在消息队列的客户端创建一个新的rpc服务器实例
	svr := ser.NewServer(new(Server), opts...)

	// 启动rpc服务器
	err := svr.Run()
	if err != nil {
		// 如果服务器启动失败，打印错误信息
		println(err.Error())
	}
}

func main() {
	consumer := client3.Consumer{}
	
	//连接一个rpc服务器
	go start_server(":8889")

	//与消息队列的服务端建立连接
	client, err := server_operations.NewClient("client", client2.WithHostPorts("0.0.0.0:8888"))
	if err != nil {
		fmt.Println(err)
	}
	consumer.Cli = client

	info := &api.InfoRequest{
		IpPort: "0.0.0.0:8889",
	}
	resp, err := client.Info(context.Background(), info)
	if(err != nil){
		fmt.Println(resp)
	}

	//test
	for {
		req := &api.PushRequest{
			Producer: int64(1),
			Topic:    "phone number",
			Key:      "yclchuxue",
			Message:  "18788888888",
		}
		resp, err := client.Push(context.Background(), req)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(resp)
		time.Sleep(5*time.Second)
	}
}
