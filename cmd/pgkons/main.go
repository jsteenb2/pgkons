package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/jsteenb2/pgkons/internal/runner"

	_ "github.com/lib/pq"
)

var debug = flag.Bool("debug", true, "turn debug on to view corresponding errors and what not in the console")

func main() {
	connect, err := runner.NewDBCFG()
	if err != nil {
		if *debug {
			log.Println(err)
			os.Exit(1)
		}
	}
	db, err := sql.Open("postgres", connect)
	if err != nil {
		check(err)
		os.Exit(1)
	}
	defer func() {
		check(db.Close())
	}()

	ctx := systemCtx()
	if err := db.PingContext(ctx); err != nil {
		log.Println(err)
		os.Exit(1)
	}

	r := runner.New(db)
	err = r.Run(ctx, *debug)
	if *debug && err != nil &&
		err != context.Canceled {
		log.Println(err)
	}
}

func check(err error) {
	if err != nil && *debug {
		log.Println(err)
	}
}

func systemCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Kill, os.Interrupt)
	go func() {
		<-sigs
		cancel()
	}()
	return ctx
}
