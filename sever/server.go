package server

import (
	"bytes"
	"errors"
	"io"
)

var errBufTooSmall = errors.New("buffer is too small to fit a single message")

type InMemory struct {
	buf []byte
}

func (s *InMemory) Write(msgs []byte) error {
	// 将新的消息数据追加到 InMemory 结构体的缓冲区中
	s.buf = append(s.buf, msgs...)
	return nil
}

func (s *InMemory) Read(off uint64, maxSize uint64, w io.Writer) error {
	maxOff := uint64(len(s.buf)) // 计算缓冲区的最大偏移量

	if off > maxOff {
		return nil // 如果起始偏移量超过了缓冲区的最大偏移量，则没有数据可读
	} else if off+maxSize >= maxOff {
		w.Write(s.buf[off:]) // 如果读取范围超过缓冲区的结尾，则写出从起始偏移量到缓冲区末尾的数据
		return nil
	}

	truncated, _, err := cutToLastMessage(s.buf[off : off+maxSize]) // 截断到最后一条完整消息
	if err != nil {
		return err // 如果截断过程出错，则返回错误
	}

	if _, err := w.Write(truncated); err != nil {
		return err // 如果写入过程出错，则返回错误
	}

	return nil // 成功读取数据并写入，返回 nil 表示没有错误
}

func (s *InMemory) Ack() error {
	s.buf = nil
	return nil
}

func cutToLastMessage(res []byte) (truncated []byte, rest []byte, err error) {
	n := len(res)

	if n == 0 {
		return res, nil, nil
	}

	if res[n-1] == '\n' {
		return res, nil, nil
	}

	lastPos := bytes.LastIndexByte(res, '\n')
	if lastPos < 0 {
		return nil, nil, errBufTooSmall
	}

	return res[0 : lastPos+1], res[lastPos+1:], nil

}
