package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/proxy"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	bind := flag.String("bind", "127.0.0.1", "address to listen on for services (0.0.0.0 for every network interface)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	c := client.New(*server)
	p := &proxy.Proxy{
		Client:   c,
		Every:    10 * time.Second,
		BindAddr: *bind,
		Services: client.NewInformer[api.Service](c, "services"),
		Pods:     client.NewInformer[api.Pod](c, "pods"),
	}

	log.Printf("proxy started")
	var wg sync.WaitGroup
	wg.Go(func() { p.Services.Run(ctx) })
	wg.Go(func() { p.Pods.Run(ctx) })
	wg.Go(func() { p.Run(ctx) })
	wg.Wait()
	log.Printf("proxy stopped")
}
