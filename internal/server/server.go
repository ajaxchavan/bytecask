package server

import (
	"crow/internal/core"
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
	delete = "del"
	ping   = "ping"
	quit   = "quit"
)

var (
	RESP_OK []byte = []byte("OK\r\n")
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
		client, _, err := syscall.Accept(serverSocket)
		if err != nil {
			store.Log.Fatal("failed to accept connection", zap.Error(err))
		}

		go handleConnection(client, store)

		//syscall.Write(clientSocket, []byte("done"))
	}

}

func handleConnection(client int, store *core.Store) {
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

		command := parts[0]
		var key string
		if len(parts) != 1 {
			key = parts[1]
		}
		store.Log.Info("command", zap.String(command, key))

		switch command {
		case ping:
			syscall.Write(client, []byte("PONG\r\n"))
			continue
		case get:
			value, err := store.Get(key)
			if err != nil {
				syscall.Write(client, []byte(err.Error()+"\r\n"))
				continue
			}
			syscall.Write(client, []byte(string(value)+"\r\n"))
		case set:
			if len(parts) < 3 {
				syscall.Write(client, []byte("Invalid command\r\n"))
				continue
			}
			err := store.Set(key, []byte(parts[2]))
			if err != nil {
				syscall.Write(client, []byte(err.Error()+"\r\n"))
				continue
			}
			syscall.Write(client, RESP_OK)
		case delete:
			if err := store.Delete(key); err != nil {
				syscall.Write(client, []byte(err.Error()+"\r\n"))
				continue
			}
			syscall.Write(client, RESP_OK)
		default:
			syscall.Write(client, []byte("Invalid command\r\n"))
		}
	}
}

//
//func temp() {
//	//store = make(map[string]string)
//
//	// Listen for incoming connections
//	listener, err := net.Listen("tcp", ":8080")
//	if err != nil {
//		fmt.Println("Error listening:", err)
//		return
//	}
//	defer listener.Close()
//
//	fmt.Println("Server listening on port 8080...")
//
//	for {
//		// Accept incoming connections
//		conn, err := listener.Accept()
//		if err != nil {
//			fmt.Println("Error accepting connection:", err)
//			continue
//		}
//
//		//go handleConnection(conn)
//		fmt.Println(conn)
//	}
//}
