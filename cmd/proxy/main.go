package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/proxy"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	bind := flag.String("bind", "127.0.0.1", "address to listen on for services (0.0.0.0 for every network interface)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	p := &proxy.Proxy{
		Client:   client.New(*server),
		Every:    time.Second,
		BindAddr: *bind,
	}

	log.Printf("proxy started")
	p.Run(ctx)
	log.Printf("proxy stopped")
}
