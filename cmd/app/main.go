package main

import (
	"context"
	"jokedb/intetnal/app"
	"jokedb/intetnal/compute"
	"jokedb/intetnal/config"
	"jokedb/intetnal/logger"
	"jokedb/intetnal/storage"
	"jokedb/intetnal/storage/engine"
	"jokedb/intetnal/tcp"
	"jokedb/intetnal/wal"
	"os"
)

func runApp() error {
	appConfig, err := config.Init(app.ConfigPah)
	if err != nil {
		return err
	}
	if err = logger.Init(appConfig.DevMode, app.Name, appConfig.Log.Level); err != nil {
		return err
	}

	wallog, err := wal.Open(
		wal.WithDirPath(appConfig.WAL.DirPath),
		wal.WithMaxSizeSegment(appConfig.WAL.MaxSizeSegment),
	)
	if err != nil {
		return err
	}

	s, err := storage.New(engine.New(), wallog, appConfig.WAL.FlushingBatchSize, appConfig.WAL.FlushingBatchTimeout)
	if err != nil {
		return err
	}

	db := app.New(compute.New(), s)

	serv, err := tcp.NewServer(appConfig.Addr, appConfig.MaxConnections, logger.L(), db.Handle)
	if err != nil {
		return err
	}

	logger.L().Infof("DB listening addr: %s", appConfig.Addr)
	serv.Listen(context.Background())

	return nil
}

func main() {
	if err := runApp(); err != nil {
		os.Exit(1)
	}
}
