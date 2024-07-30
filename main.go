package main

import (
	api "ClyMQ/kitex_gen/api/operations"
	"log"
)

func main() {
	svr := api.NewServer(new(OperationsImpl))//创建一个rpc服务

	err := svr.Run()//运行rpc

	if err != nil {
		log.Println(err.Error())
	}
}
