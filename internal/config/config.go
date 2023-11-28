package config

import "time"

var (
	defaultSyncInterval     = time.Minute * 1
	defaultMergeInterval    = time.Hour * 6
	defaultFileSizeInterval = time.Minute * 1
)

type Opts struct {
	Dir                   string
	fsync                 bool
	syncInterval          *time.Duration
	mergeInterval         *time.Duration
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
		syncInterval:          &defaultSyncInterval,
		mergeInterval:         &defaultMergeInterval,
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
