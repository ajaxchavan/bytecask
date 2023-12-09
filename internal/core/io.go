package core

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ajaxchavan/crow/internal/config"
)

type RespParser struct {
	c   io.ReadWriter
	buf *bytes.Buffer
	p   []byte
}

func NewParser(c io.ReadWriter) (*RespParser, error) {
	return NewParserWithBytes(c, []byte{})
}

func NewParserWithBytes(c io.ReadWriter, bt []byte) (*RespParser, error) {
	buf := bytes.NewBuffer([]byte{})
	buf.Write(bt)
	return &RespParser{
		c:   c,
		buf: buf,
		p:   make([]byte, config.IOBufferLength),
	}, nil
}

func (r *RespParser) readString() (string, error) {
	line, err := r.buf.ReadString('\r')
	if err != nil && err != io.EOF {
		return "", err
	}

	// skip '\r'
	return line[:len(line)-1], nil
}

func (r *RespParser) Decode() (string, error) {
	for {
		n, err := r.c.Read(r.p)
		if n <= 0 {
			break
		}

		r.buf.Write(r.p[:n])
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}

		if bytes.Contains(r.p, []byte{'\r', '\n'}) {
			break
		}

		if r.buf.Len() >= config.IOBufferLengthMax {
			return "", fmt.Errorf("input too long. max input can be %d bytes", config.IOBufferLengthMax)
		}
	}
	//TODO: implement https://bou.ke/blog/hacking-developers

	return r.readString()
}
