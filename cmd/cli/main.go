package main

import (
	"bufio"
	"context"
	"fmt"
	"jokedb/intetnal/app"
	"jokedb/intetnal/compute"
	"jokedb/intetnal/storage/engine"
	"os"
)

func main() {
	ctx := context.Background()
	db := app.New(compute.New(), engine.New())

	sc := bufio.NewScanner(os.Stdin)
	sc.Split(bufio.ScanLines)
	for sc.Scan() {
		res, err := db.DoRawCommand(ctx, sc.Text())
		if err != nil {
			fmt.Fprintln(os.Stdout, err)
		} else {
			fmt.Fprintln(os.Stdout, res)
		}
	}
}
