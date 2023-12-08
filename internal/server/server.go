package server

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/ajaxchavan/crow/internal/core"
)

func WaitForSignal(wg *sync.WaitGroup, sigch chan os.Signal, store *core.Store) {
	defer wg.Done()

	<-sigch

	store.Shutdown()

	os.Exit(0)
}

func RunServer(wg *sync.WaitGroup, store *core.Store) {
	defer wg.Done()

	serverSocket, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		store.Log.Fatal("failed to create socket", zap.Error(err))
	}

	serverAddr := &syscall.SockaddrInet4{Port: 6969}
	copy(serverAddr.Addr[:], net.ParseIP("127.0.0.1").To4())

	//ip := net.ParseIP("0.0.0.0")

	if err := syscall.Bind(serverSocket, serverAddr); err != nil {
		store.Log.Fatal("failed to bind socket", zap.Error(err))
	}

	if err := syscall.Listen(serverSocket, 100); err != nil {
		store.Log.Fatal("failed to connect server", zap.Error(err))
	}

	defer syscall.Close(serverSocket)
	for {
		fd, _, err := syscall.Accept(serverSocket)
		if err != nil {
			store.Log.Fatal("failed to accept connection", zap.Error(err))
		}

		go handleConnection(fd, store)

		//syscall.Write(fdSocket, []byte("done"))
	}

}

func handleConnection(fd int, store *core.Store) {
	defer syscall.Close(fd)
	for {
		client := core.NewClient(fd)
		cmd, err := readCmd(client)
		if err != nil {
			const msg = "failed to read cmd"
			store.Log.Error(msg, zap.Error(err))
			continue
		}
		response(store, cmd, client)
	}
}

func readCmd(c io.ReadWriter) (*core.CrowCmd, error) {
	rp, err := core.NewParser(c)
	if err != nil {
		return nil, fmt.Errorf("unable to build parser %s", err)
	}
	p, err := rp.Decode()
	if err != nil {
		return nil, fmt.Errorf("unable to decode %s", err)
	}

	tokens := core.ToArrayString(p)
	return &core.CrowCmd{
		Cmd:  strings.ToUpper(tokens[0]),
		Args: tokens[1:],
	}, nil
}

func response(store *core.Store, cmd *core.CrowCmd, client *core.Client) {
	store.EvalAndResponse(cmd, client)
}
