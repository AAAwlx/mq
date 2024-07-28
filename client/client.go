package client

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
)


// 定义默认的缓冲区大小为64KB
const defaultScratchSize = 64 * 1024
type Simple struct{
	addrs []string		// 服务器ip
	cl     *http.Client	// HTTP 客户端，用于执行网络请求
	offset uint64		// 偏移量，用于跟踪当前处理的位置
}

//创建一个 Simple 结构体，并初始化 addrs 为传入的 addrs
func NewSimple(addrs[] string)* Simple{
	return &Simple{
		addrs: addrs,
		cl:    &http.Client{},
	}
}

//发送功能，向消息队列的服务器发送消息
func (s* Simple) Send(msgs []byte) error{
	    // 将消息写入缓冲区
	res, err := s.cl.Post(s.addrs[0]+"/write", "application/octet-stream", bytes.NewReader(msgs))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return fmt.Errorf("http code %d, %s", res.StatusCode, b.String())
	}

	io.Copy(io.Discard, res.Body)
	return nil
}

//接收功能，接收来自消息队列的消息
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultScratchSize)
	}

	addrIndex := rand.Intn(len(s.addrs))
	addr := s.addrs[addrIndex]
	readURL := fmt.Sprintf("%s/read?off=%d&maxSize=%d", addr, s.offset, len(scratch))

	res, err := s.cl.Get(readURL)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return nil, fmt.Errorf("http code %d, %s", res.StatusCode, b.String())
	}
	b := bytes.NewBuffer(scratch[0:0])
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return nil, err
	}

	// 读0个字节但没错，意味着按照约定文件结束
	if b.Len() == 0 {
		if err := s.ackCurrentChunk(addr); err != nil {
			return nil, err

		}
		return nil, io.EOF
	}

	s.offset += uint64(b.Len())
	return b.Bytes(), nil
}

func (s *Simple) ackCurrentChunk(addr string) error {
	// 发送 HTTP GET 请求到指定地址的 /ack 端点
	res, err := s.cl.Get(addr + "/ack")
	if err != nil {
		return err
	}

	// 确保在函数结束时关闭响应体，以防止资源泄漏
	defer res.Body.Close()

	// 检查响应状态码是否为 200 OK，表示请求成功
	if res.StatusCode != http.StatusOK {
		// 如果状态码不是 200 OK，读取并返回响应体中的错误信息
		var b bytes.Buffer
		io.Copy(&b, res.Body)
		return fmt.Errorf("http code %d, %s", res.StatusCode, b.String())
	}

	// 如果状态码是 200 OK，将响应体中的数据丢弃
	io.Copy(io.Discard, res.Body)
	// 成功处理后返回 nil，表示没有错误
	return nil
}
