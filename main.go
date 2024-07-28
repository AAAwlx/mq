package main

import (
	"fmt"
	"log"
	"github.com/ylx/mq/server"
	"github.com/ylx/mq/web"
)

func main() {
	s := web.NewServer(&server.InMemory{})
	log.Printf("Listening connections")

	err := s.Serve()
	if err != nil {
		fmt.Printf(err.Error())
	}
}
