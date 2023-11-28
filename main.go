package main

import (
	"crow/internal/config"
	"crow/internal/core"
	"crow/internal/log"
	"crow/internal/server"
	"fmt"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {

	//TODO: setupFlags()

	logger, err := log.NewLogger()
	if err != nil {
		fmt.Println("Failed to initialize ")
		os.Exit(1)
	}
	logger.Info("Spotting a crow \U0001F98B")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	var wg sync.WaitGroup
	wg.Add(1)

	cfg := config.NewConfig()

	store, err := core.New(*cfg, *logger)
	if err != nil {
		logger.Fatal("failed to create store object", zap.Error(err))
	}

	go server.RunServer(&wg, store)

	go server.WaitForSignal(&wg, sigs, store)

	wg.Wait()
}
