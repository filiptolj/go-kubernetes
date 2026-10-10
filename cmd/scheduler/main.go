package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/leader"
	"github.com/filiptolj/go-kubernetes/pkg/scheduler"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	strategy := flag.String("strategy", "least-loaded", "how to pick nodes: least-loaded or round-robin")
	elect := flag.Bool("leader-elect", true, "take part in leader election, so only one scheduler works at a time")
	id := flag.String("id", leader.DefaultIdentity(), "this scheduler's name in the leader election")
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	c := client.New(*server)
	log.Printf("scheduler started (strategy: %s)", *strategy)

	// run schedules until ctx is cancelled. The informers keep reconnecting
	// if the API server goes away.
	run := func(ctx context.Context) {
		s := &scheduler.Scheduler{
			Client: c,
			Picker: picker,
			Pods:   client.NewInformer[api.Pod](c, "pods"),
			Nodes:  client.NewInformer[api.Node](c, "nodes"),
			Resync: 10 * time.Second, // retry pods that couldn't be scheduled
		}
		var wg sync.WaitGroup
		wg.Go(func() { s.Pods.Run(ctx) })
		wg.Go(func() { s.Nodes.Run(ctx) })
		wg.Go(func() { s.Run(ctx) })
		wg.Wait()
	}

	if !*elect {
		run(ctx)
	} else {
		// Several schedulers may run; the one holding the lease schedules,
		// and the others wait to take over.
		e := &leader.Elector{
			Client: c, Namespace: api.DefaultNamespace, Name: "scheduler", Identity: *id,
			LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RenewEvery: 2 * time.Second,
		}
		e.Run(ctx, run)
	}
	log.Printf("scheduler stopped")
}
