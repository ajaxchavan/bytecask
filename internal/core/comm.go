package core

import (
	"io"
	"strings"
	"syscall"
)

type Client struct {
	io.ReadWriter
	fd int
}

func NewClient(fd int) *Client {
	return &Client{
		fd: fd,
	}
}

func (c *Client) Write(p []byte) (int, error) {
	return syscall.Write(c.fd, p)
}

func (c *Client) Read(p []byte) (int, error) {
	return syscall.Read(c.fd, p)
}

func ToArrayString(line string) []string {
	return strings.Split(line, " ")
}
