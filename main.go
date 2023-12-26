package main

import (
	"context"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/ajaxchavan/bytecask/internal/config"
	"github.com/ajaxchavan/bytecask/internal/core"
	"github.com/ajaxchavan/bytecask/internal/log"
	"github.com/ajaxchavan/bytecask/internal/server"
)

func main() {
	//TODO: setupFlags()
	hint := flag.Bool("hint", false, "specify to build keydir from scratch and not to use hint_file")
	flag.Parse()

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, err := log.NewLogger()
	if err != nil {
		fmt.Println("Failed to initialize ")
		os.Exit(1)
	}

	logger.Info("Infusing brilliance in bytes! \U0001F4BE")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	var wg sync.WaitGroup

	cfg := config.NewConfig()

	store, err := core.New(*cfg, *logger, *hint)
	if err != nil {
		logger.Fatal("failed to create store object", zap.Error(err))
	}

	wg.Add(1)
	go server.RunServer(ctx, &wg, store)

	wg.Add(1)
	go store.AsyncFlush(ctx, &wg)

	wg.Add(1)
	go store.Compact(ctx, &wg)

	wg.Add(1)
	go store.UpdateActiveDatafile(ctx, &wg)

	<-signals
	logger.Info("shutting down....")

	// tell goroutines to stop
	cancel()
	store.Shutdown()

	// wait them all to reply back.
	wg.Wait()
}
