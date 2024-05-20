package main

import (
	"context"
	"jokedb/intetnal/app"
	"jokedb/intetnal/compute"
	"jokedb/intetnal/config"
	"jokedb/intetnal/logger"
	"jokedb/intetnal/storage/engine"
	"jokedb/intetnal/tcp"
	"os"
)

func runApp() error {
	appConfig, err := config.Init(app.ConfigPah)
	if err != nil {
		return err
	}
	err = logger.Init(appConfig.DevMode, app.Name, appConfig.Log.Level)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := app.New(compute.New(), engine.New())

	serv, err := tcp.NewServer(appConfig.Addr, appConfig.MaxConnections, logger.L(), db.Handle)
	if err != nil {
		return err
	}

	logger.L().Infof("DB listening addr: %s", appConfig.Addr)
	serv.Listen(ctx)

	return nil
}

func main() {
	if err := runApp(); err != nil {
		os.Exit(1)
	}
}
