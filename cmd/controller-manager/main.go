package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/controller"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	c := client.New(*server)

	nodes := &controller.NodeController{
		Client:  c,
		Timeout: 15 * time.Second,
		Every:   5 * time.Second,
	}
	replicaSets := &controller.ReplicaSetController{
		Client:       c,
		Resync:       2 * time.Second,
		BackoffBase:  10 * time.Second,
		BackoffMax:   2 * time.Minute,
		HealthyAfter: time.Minute,
	}
	deployments := &controller.DeploymentController{Client: c, Every: time.Second}
	jobs := &controller.JobController{Client: c, Every: time.Second}
	cronJobs := &controller.CronJobController{Client: c, Every: 5 * time.Second}
	daemonSets := &controller.DaemonSetController{Client: c, Every: 2 * time.Second}
	statefulSets := &controller.StatefulSetController{Client: c, Every: time.Second}

	log.Printf("controller-manager started")

	// Run the controllers at the same time, and wait until all have stopped.
	// They only stop on Ctrl+C: if the API server goes away, they keep retrying.
	var wg sync.WaitGroup
	wg.Go(func() {
		nodes.Run(ctx)
	})
	wg.Go(func() {
		replicaSets.Run(ctx)
	})
	wg.Go(func() {
		deployments.Run(ctx)
	})
	wg.Go(func() {
		jobs.Run(ctx)
	})
	wg.Go(func() {
		cronJobs.Run(ctx)
	})
	wg.Go(func() {
		daemonSets.Run(ctx)
	})
	wg.Go(func() {
		statefulSets.Run(ctx)
	})
	wg.Wait()

	log.Printf("controller-manager stopped")
}
