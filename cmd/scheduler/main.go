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
	"github.com/filiptolj/go-kubernetes/pkg/scheduler"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	strategy := flag.String("strategy", "least-loaded", "how to pick nodes: least-loaded or round-robin")
	flag.Parse()

	var picker scheduler.Picker
	switch *strategy {
	case "least-loaded":
		picker = &scheduler.LeastLoaded{}
	case "round-robin":
		picker = &scheduler.RoundRobin{}
	default:
		log.Fatalf("unknown strategy %q", *strategy)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	c := client.New(*server)
	s := &scheduler.Scheduler{
		Client: c,
		Picker: picker,
		Pods:   client.NewInformer[api.Pod](c, "pods"),
		Nodes:  client.NewInformer[api.Node](c, "nodes"),
		Resync: 10 * time.Second, // retry pods that couldn't be scheduled
	}
	log.Printf("scheduler started (strategy: %s)", *strategy)

	// The informers keep reconnecting if the API server goes away, so this
	// runs until Ctrl+C.
	var wg sync.WaitGroup
	wg.Go(func() { s.Pods.Run(ctx) })
	wg.Go(func() { s.Nodes.Run(ctx) })
	wg.Go(func() { s.Run(ctx) })
	wg.Wait()
	log.Printf("scheduler stopped")
}
