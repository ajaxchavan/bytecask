package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/ajaxchavan/bytecask/internal/core"
)

func RunServer(ctx context.Context, wg *sync.WaitGroup, store *core.Store) {
	defer wg.Done()

	serverSocket, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		store.Log.Fatal("failed to create socket", zap.Error(err))
	}

	if err := syscall.SetNonblock(serverSocket, true); err != nil {
		store.Log.Fatal("failed to set nonblocking socket")
	}

	serverAddr := &syscall.SockaddrInet4{Port: 6969}
	copy(serverAddr.Addr[:], net.ParseIP("127.0.0.1").To4())

	if err := syscall.SetsockoptInt(serverSocket, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		store.Log.Fatal("failed to set SO_REUSEADDR:", zap.Error(err))
	}

	if err := syscall.Bind(serverSocket, serverAddr); err != nil {
		store.Log.Fatal("failed to bind socket", zap.Error(err))
	}

	if err := syscall.Listen(serverSocket, 100); err != nil {
		store.Log.Fatal("failed to connect server", zap.Error(err))
	}

	defer syscall.Close(serverSocket)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fd, _, err := syscall.Accept(serverSocket)
			if err != nil {
				var errno syscall.Errno
				if errors.As(err, &errno) && (errno == syscall.EAGAIN || errno == syscall.EWOULDBLOCK) {
					continue
				}
				select {
				case <-ctx.Done():
					// Context canceled while waiting for Accept, exit the goroutine
					return
				default:
					store.Log.Fatal("failed to accept connection", zap.Error(err))
				}
			}

			// Make the accepted socket non-blocking
			if err := syscall.SetNonblock(fd, true); err != nil {
				store.Log.Fatal("failed to set socket to non-blocking", zap.Error(err))
			}

			go handleConnection(ctx, fd, store)
		}
	}

}

func handleConnection(ctx context.Context, fd int, store *core.Store) {
	defer syscall.Close(fd)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			client := core.NewClient(fd)
			cmd, err := readCmd(client)
			if err != nil {
				_, _ = client.Write([]byte("invalid cmd\r\n"))
				const msg = "failed to read cmd"
				store.Log.Error(msg, zap.Error(err))
				continue
			}
			if cmd == nil {
				_, _ = client.Write([]byte("syntax error\r\n"))
				continue
			}

			response(store, cmd, client)
		}
	}
}

func readCmd(c io.ReadWriter) (*core.Cmd, error) {
	rp, err := core.NewParser(c)
	if err != nil {
		return nil, fmt.Errorf("unable to build parser %s", err)
	}
	p, err := rp.Decode()
	if err != nil {
		return nil, fmt.Errorf("unable to decode %s", err)
	}

	return core.NewCmd(p), nil
}

func response(store *core.Store, cmd *core.Cmd, client *core.Client) {
	store.EvalAndResponse(cmd, client)
}
