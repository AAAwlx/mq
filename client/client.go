package client

import "bytes"

// 定义默认的缓冲区大小为64KB
const defaultScratchSize = 64 * 1024
type Simple struct{
	addrs []string
	buf bytes.Buffer
}

//创建一个 Simple 结构体，并初始化 addrs 为传入的 addrs
func NewSimple(addrs[] string)* Simple{
	return &Simple{
		addrs: addrs,
	}
}

//发送功能，向消息队列的服务器发送消息
func (s* Simple) Send(msgs []byte) error{
	    // 将消息写入缓冲区
		_, err := s.buf.Write(msgs)
		return err
}

//接收功能，接收来自消息队列的消息
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
    // 如果传入的 scratch 缓冲区为 nil，则创建一个默认大小的缓冲区
    if scratch == nil {
        scratch = make([]byte, defaultScratchSize)
    }

    // 从缓冲区读取数据到 scratch 缓冲区中
    n, err := s.buf.Read(scratch)
    if err != nil {
        return nil, err
    }

    // 返回读取的有效数据部分和 nil 错误
    return scratch[0:n], nil
}
