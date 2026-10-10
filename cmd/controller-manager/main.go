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
	"github.com/filiptolj/go-kubernetes/pkg/controller"
	"github.com/filiptolj/go-kubernetes/pkg/leader"
)

func main() {
	server := flag.String("server", "http://localhost:8080", "API server address")
	elect := flag.Bool("leader-elect", true, "take part in leader election, so only one controller-manager works at a time")
	id := flag.String("id", leader.DefaultIdentity(), "this controller-manager's name in the leader election")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	c := client.New(*server)
	log.Printf("controller-manager started")

	if !*elect {
		runControllers(ctx, c)
	} else {
		// Several controller-managers may run; the one holding the lease
		// works, and the others wait to take over.
		e := &leader.Elector{
			Client: c, Namespace: api.DefaultNamespace, Name: "controller-manager", Identity: *id,
			LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RenewEvery: 2 * time.Second,
		}
		e.Run(ctx, func(ctx context.Context) { runControllers(ctx, c) })
	}
	log.Printf("controller-manager stopped")
}

// runControllers runs every controller until ctx is cancelled.
func runControllers(ctx context.Context, c *client.Client) {
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
	garbage := &controller.GarbageCollector{Client: c, Informers: informers, Every: 30 * time.Second}
	autoscalers := &controller.AutoscalerController{
		Client:    c,
		Informers: informers,
		Every:     15 * time.Second,
		// Kubernetes waits 5 minutes before shrinking; 1 is less dull to watch.
		DownscaleWindow: time.Minute,
	}

	// Run the controllers at the same time, and wait until all have stopped.
	// They stop when ctx is cancelled: if the API server goes away, they keep retrying.
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
	wg.Go(func() {
		garbage.Run(ctx)
	})
	wg.Go(func() {
		autoscalers.Run(ctx)
	})
	wg.Wait()

}
