package core

import (
	"errors"
	"io"
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
	for {
		n, err := syscall.Read(c.fd, p)
		if err != nil {
			var errno syscall.Errno
			if errors.As(err, &errno) && (errno == syscall.EAGAIN || errno == syscall.EWOULDBLOCK) {
				// EAGAIN or EWOULDBLOCK means no data available right now, try again
				continue
			}
			return n, err // Return the result of the Read operation, even if it's an error
		}
		return n, err
	}
}
