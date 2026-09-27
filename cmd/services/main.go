package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gutrovalim/akaiito-application-test/internal/services"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: services <recorder|intake|ledger|metrics> [--addr :8080] [--dir /data] [--upstream URL]")
		os.Exit(2)
	}
	role := os.Args[1]
	fs := flag.NewFlagSet(role, flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	dir := fs.String("dir", "/data", "run directory")
	upstream := fs.String("upstream", "http://intake:8080", "recorder upstream")
	fs.Parse(os.Args[2:])
	var (
		h   http.Handler
		err error
	)
	switch role {
	case "recorder":
		h, err = services.Recorder(filepath.Join(*dir, "recorder"), *upstream)
	case "intake":
		h, err = services.Intake(filepath.Join(*dir, "intake"))
	case "ledger":
		h, err = services.Ledger(filepath.Join(*dir, "ledger"))
	case "metrics":
		h, err = services.Metrics(filepath.Join(*dir, "metrics"))
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q\n", role)
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s listening on %s, dir %s", role, *addr, *dir)
	log.Fatal(http.ListenAndServe(*addr, h))
}
