package main

import (
	"bufio"
	"flag"
	"fmt"
	"jokedb/intetnal/app"
	"jokedb/intetnal/config"
	"jokedb/intetnal/logger"
	"jokedb/intetnal/tcp"
	"os"
)

func runClient() error {
	addr := flag.String("addr", app.Addr, "listening addr")
	flag.Parse()

	conf, err := config.Init(app.ConfigPah)
	if err != nil {
		return err
	}

	err = logger.Init(conf.DevMode, app.Name, conf.Log.Level)
	if err != nil {
		return err
	}

	cl, err := tcp.NewClient(*addr, logger.L())
	if err != nil {
		return err
	}
	defer cl.Close()

	sc := bufio.NewScanner(os.Stdin)
	sc.Split(bufio.ScanLines)

	for sc.Scan() {
		resp, errPub := cl.Publish(sc.Bytes())
		if errPub != nil {
			logger.L().Error(errPub)
			return errPub
		}

		fmt.Fprintln(os.Stdout, string(resp))
	}

	return nil
}

func main() {
	if err := runClient(); err != nil {
		os.Exit(1)
	}
}
