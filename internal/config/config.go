package config

import "time"

var (
	defaultSyncInterval     = time.Minute * 1
	defaultMergeInterval    = time.Hour * 6
	defaultFileSizeInterval = time.Minute * 1
)

const (
	IOBufferLength    = 512
	IOBufferLengthMax = 50 * 1024
)

type Opts struct {
	Dir                   string
	fsync                 bool
	SyncInterval          time.Duration
	MergeInterval         time.Duration
	checkFileSizeInterval *time.Duration
}

type Config struct {
	Opts
}

type OptFunc func(*Opts)

func defaultOpts() Opts {
	return Opts{
		Dir:                   ".data",
		fsync:                 false,
		SyncInterval:          defaultSyncInterval,
		MergeInterval:         defaultMergeInterval,
		checkFileSizeInterval: &defaultFileSizeInterval,
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
