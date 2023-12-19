package config

import (
	"os"
	"time"
)

var (
	// debug
	defaultSyncInterval           = time.Minute * 1
	defaultMergeInterval          = time.Minute * 3
	defaultDatafileChangeInterval = time.Minute * 2
)

const (
	IOBufferLength    = 512
	IOBufferLengthMax = 50 * 1024
)

type Opts struct {
	Dir                    string
	Path                   string
	fsync                  bool
	SyncInterval           time.Duration
	MergeInterval          time.Duration
	DatafileChangeInterval time.Duration
}

type Config struct {
	Opts
}

type OptFunc func(*Opts)

func defaultOpts() Opts {
	wd, _ := os.Getwd()
	return Opts{
		Dir:                    ".data",
		Path:                   wd,
		fsync:                  false,
		SyncInterval:           defaultSyncInterval,
		MergeInterval:          defaultMergeInterval,
		DatafileChangeInterval: defaultDatafileChangeInterval,
	}
}

func NewConfig(opts ...OptFunc) *Config {
	o := defaultOpts()
	for _, fn := range opts {
		fn(&o)
	}
	return &Config{
		o,
	}
}
