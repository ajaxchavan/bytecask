package server

import (
	"crow/internal/log"
	"fmt"
	"go.uber.org/zap"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
)

const (
	get    = "get"
	set    = "set"
	delete = "delete"
	ping   = "ping"
	quit   = "quit"
)

func WaitForSignal(wg *sync.WaitGroup, sigch chan os.Signal) {
	defer wg.Done()

	<-sigch

	os.Exit(0)
}

func RunServer(wg *sync.WaitGroup, store *core.Store, logger log.Log) {
	defer wg.Done()

	serverSocket, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		logger.Fatal("failed to create socket", zap.Error(err))
	}

	serverAddr := &syscall.SockaddrInet4{Port: 6969}
	copy(serverAddr.Addr[:], net.ParseIP("127.0.0.1").To4())

	//ip := net.ParseIP("0.0.0.0")

	if err := syscall.Bind(serverSocket, serverAddr); err != nil {
		logger.Fatal("failed to bind socket", zap.Error(err))
	}

	if err := syscall.Listen(serverSocket, 100); err != nil {
		logger.Fatal("failed to connect server", zap.Error(err))
	}

	defer syscall.Close(serverSocket)
	for {
		client, _, err := syscall.Accept(serverSocket)
		if err != nil {
			logger.Fatal("failed to accept connection", zap.Error(err))
		}

		go handleConnection(client, store, logger)

		//syscall.Write(clientSocket, []byte("done"))
	}

}

func handleConnection(client int, store *core.Store, logger log.Log) {
	defer syscall.Close(client)

	for {
		// Read from the socket
		buffer := make([]byte, 1024)
		n, err := syscall.Read(client, buffer)
		if err != nil {
			fmt.Println("Error reading from socket:", err)
			return
		}

		request := string(buffer[:n-2])

		// Parse the request and perform the corresponding action
		parts := strings.Split(request, " ")
		if len(parts) < 2 {
			fmt.Println("Invalid request:", request)
			syscall.Write(client, []byte("Invalid command"))
			continue
		}

		command := parts[0]
		key := parts[1]
		logger.Info("command", zap.String(command, key))
		switch command {
		case ping:

		case get:
			logger.Info("get")
			value, err := store.Get(key)
			if err != nil {
				syscall.Write(client, []byte(err.Error()))
				continue
			}
			logger.Info("write")
			syscall.Write(client, []byte(string(value)+"\n"))
		case set:
			if len(parts) < 3 {
				syscall.Write(client, []byte("Invalid command"))
				continue
			}
			logger.Info("set", zap.String(key, parts[2]))
			err := store.Set(key, []byte(parts[2]))
			if err != nil {
				syscall.Write(client, []byte(err.Error()))
				continue
			}
			syscall.Write(client, []byte("OK\n"))
		}
		//syscall.Write(client, []byte(fmt.Sprintf("%s: %s", command, key)))
	}
}

func temp() {
	//store = make(map[string]string)

	// Listen for incoming connections
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server listening on port 8080...")

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		//go handleConnection(conn)
		fmt.Println(conn)
	}
}
