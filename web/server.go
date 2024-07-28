package web

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"yerfYar/server"
)

// defaultBufSize 定义了读取数据时的默认缓冲区大小
const defaultBufSize = 512 * 1024

// Server 结构体表示一个处理客户端请求的服务器
type Server struct {
	s *server.InMemory // 内存中的数据存储
}

// NewServer 函数创建一个新的 Server 实例
func NewServer(s *server.InMemory) *Server {
	return &Server{s: s} // 返回一个包含给定 InMemory 存储的 Server 实例
}

// handler 方法处理所有传入的 HTTP 请求
func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) { // 根据请求路径选择相应的处理函数
	case "/write":
		s.writeHandler(ctx) // 处理写请求
	case "/read":
		s.readHandler(ctx) // 处理读请求
	case "/ack":
		s.ackHandler(ctx) // 处理确认请求
	default:
		ctx.WriteString("hello yerfYar!") // 对于未知的路径，返回默认响应
	}
}

// writeHandler 方法处理写操作的请求
func (s *Server) writeHandler(ctx *fasthttp.RequestCtx) {
	ctx.WriteString("you chose to write...") // 响应写操作
	if err := s.s.Write(ctx.Request.Body()); err != nil { // 将请求体写入内存存储
		ctx.SetStatusCode(fasthttp.StatusInternalServerError) // 如果发生错误，设置HTTP状态码为500
		ctx.WriteString(err.Error()) // 返回错误信息
	}
}

// ackHandler 方法处理确认操作的请求
func (s *Server) ackHandler(ctx *fasthttp.RequestCtx) {
	if err := s.s.Ack(); err != nil { // 调用内存存储的确认方法
		ctx.SetStatusCode(fasthttp.StatusInternalServerError) // 如果发生错误，设置HTTP状态码为500
		ctx.WriteString(err.Error()) // 返回错误信息
	}
}

// readHandler 方法处理读取操作的请求
func (s *Server) readHandler(ctx *fasthttp.RequestCtx) {
	// 从请求参数中获取读取的起始偏移量
	off, err := ctx.QueryArgs().GetUint("off")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest) // 如果参数无效，设置HTTP状态码为400
		ctx.WriteString(fmt.Sprintf("bad `off` GET param: %v", err))
		return
	}

	// 从请求参数中获取最大读取大小
	maxSize, err := ctx.QueryArgs().GetUint("maxSize")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest) // 如果参数无效，设置HTTP状态码为400
		ctx.WriteString(fmt.Sprintf("bad `maxSize` GET param: %v", err))
		return
	}

	// 读取数据并写入响应
	err = s.s.Read(uint64(off), uint64(maxSize), ctx)
	if err != nil && err != io.EOF {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError) // 如果发生错误，设置HTTP状态码为500
		ctx.WriteString(err.Error()) // 返回错误信息
		return
	}
}

// Serve 方法启动服务器并监听请求
func (s *Server) Serve() error {
	return fasthttp.ListenAndServe(":8080", s.handler) // 在8080端口启动服务器
}
