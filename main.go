package main

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/ajaxchavan/crow/internal/config"
	"github.com/ajaxchavan/crow/internal/core"
	"github.com/ajaxchavan/crow/internal/log"
	"github.com/ajaxchavan/crow/internal/server"
)

func main() {
	//TODO: setupFlags()
	hint := flag.Bool("hint", false, "specify to build keydir from scratch and not to use hint_file")
	flag.Parse()

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

	store, err := core.New(*cfg, *logger, *hint)
	if err != nil {
		logger.Fatal("failed to create store object", zap.Error(err))
	}

	go server.RunServer(&wg, store)

	go server.WaitForSignal(&wg, sigs, store)

	go store.AsyncFlush(&wg)

	go store.Compact(&wg)

	wg.Wait()
}
