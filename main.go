package main

import (
	Server "mq/server"
	"fmt"
	"net"

	"github.com/cloudwego/kitex/server"
)

func main() {

	addr,_ := net.ResolveTCPAddr("tcp", ":8888")//获取ip
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	rpcServer := new(Server.RPCServer)//启动一个Broker实例
	
	err := rpcServer.Start(opts)
	if err != nil {
		fmt.Println(err)
	}
}