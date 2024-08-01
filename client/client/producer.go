package client

import "mq/kitex_gen/api/server_operations"

type Producer struct{
	Cli server_operations.Client
}

