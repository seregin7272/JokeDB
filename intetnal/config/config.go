package config

import (
	"jokedb/intetnal/app"

	"github.com/spf13/viper"
)

type Config struct {
	Engine         Engine
	Log            Log
	MaxConnections uint8
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
			Output: "./log/app.log",
		},
	}
	viper.SetConfigFile(configFile)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
