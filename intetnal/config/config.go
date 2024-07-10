package config

import (
	"jokedb/intetnal/app"
	"time"

	"github.com/spf13/viper"
)

const (
	maxSizeSegment       = 1024 * 1024 * 10
	flushingBatchSize    = 1000
	flushingBatchTimeout = 10 * time.Millisecond
)

type Config struct {
	Engine         Engine
	Log            Log
	WAL            WAL
	MaxConnections uint
	Addr           string
	DevMode        bool
}

type Conn struct {
	Port int
	Host string
}

type Engine struct {
	Type string
}

type Log struct {
	Level  string
	Output string
}

type WAL struct {
	Enabled              bool
	DirPath              string
	MaxSizeSegment       uint32
	FlushingBatchSize    uint32
	FlushingBatchTimeout time.Duration
}

func Init(configFile string) (*Config, error) {
	config := Config{
		Addr: app.Addr,
		Engine: Engine{
			Type: "in_memory",
		},
		MaxConnections: app.MaxConn,
		DevMode:        false,
		Log: Log{
			Level:  "error",
			Output: "./db/log/app.log",
		},
		WAL: WAL{
			Enabled:              true,
			MaxSizeSegment:       maxSizeSegment,
			DirPath:              "./db/wal",
			FlushingBatchSize:    flushingBatchSize,
			FlushingBatchTimeout: flushingBatchTimeout,
		},
	}
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
