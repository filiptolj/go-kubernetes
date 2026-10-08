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

	// One cache per kind, shared by all the controllers. They react to
	// changes as they arrive; the intervals below are only a safety net, and
	// for things that depend on time passing.
	informers := controller.NewInformers(c)

	nodes := &controller.NodeController{
		Client:    c,
		Informers: informers,
		Timeout:   15 * time.Second,
		Every:     5 * time.Second,
	}
	replicaSets := &controller.ReplicaSetController{
		Client:       c,
		Informers:    informers,
		Resync:       2 * time.Second, // also how soon a backoff that ran out is noticed
		BackoffBase:  10 * time.Second,
		BackoffMax:   2 * time.Minute,
		HealthyAfter: time.Minute,
	}
	deployments := &controller.DeploymentController{Client: c, Informers: informers, Every: 10 * time.Second}
	jobs := &controller.JobController{Client: c, Informers: informers, Every: 10 * time.Second}
	cronJobs := &controller.CronJobController{Client: c, Informers: informers, Every: 5 * time.Second}
	daemonSets := &controller.DaemonSetController{Client: c, Informers: informers, Every: 10 * time.Second}
	statefulSets := &controller.StatefulSetController{Client: c, Informers: informers, Every: 10 * time.Second}

	log.Printf("controller-manager started")

	// Run the controllers at the same time, and wait until all have stopped.
	// They only stop on Ctrl+C: if the API server goes away, they keep retrying.
	var wg sync.WaitGroup
	wg.Go(func() {
		informers.Run(ctx)
	})
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
