package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
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
	log.Printf("scheduler started (strategy: %s)", *strategy)

	// run only returns when the connection to the API server breaks, so this
	// keeps reconnecting until Ctrl+C.
	client.Retry(ctx, "scheduler", func() error {
		return run(ctx, c, picker)
	})
	log.Printf("scheduler stopped")
}

// run schedules the pods that already exist, then every new pod as it arrives.
// Every 10 seconds it also retries pods that couldn't be scheduled yet, for
// example because there were no ready nodes. It returns when the watch ends.
func run(ctx context.Context, c *client.Client, picker scheduler.Picker) error {
	// Start watching before listing, so no pod can slip through the gap between them.
	events, err := c.WatchPods(ctx)
	if err != nil {
		return err
	}

	resync := time.NewTicker(10 * time.Second)
	defer resync.Stop()

	scheduleAll(c, picker)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return errors.New("lost connection to the API server")
			}
			if event.Type == api.EventAdded {
				schedule(c, picker, event.Pod)
			}
		case <-resync.C:
			scheduleAll(c, picker)
		}
	}
}

// scheduleAll schedules every pod that is still waiting for a node.
func scheduleAll(c *client.Client, picker scheduler.Picker) {
	pods, err := c.ListPods("") // every namespace
	if err != nil {
		log.Printf("cannot list pods: %v", err)
		return
	}
	for _, pod := range pods {
		schedule(c, picker, pod)
	}
}

// schedule picks a node for one pod and binds the pod to it.
// Pods that already have a node, or that are no longer pending, are skipped.
func schedule(c *client.Client, picker scheduler.Picker, pod api.Pod) {
	if pod.NodeName != "" || pod.Phase != api.PodPending {
		return
	}

	nodes, err := c.ListNodes()
	if err != nil {
		log.Printf("cannot schedule pod %s: %v", api.Key(pod.Namespace, pod.Name), err)
		return
	}

	pods, err := c.ListPods("") // a node's load counts pods from every namespace
	if err != nil {
		log.Printf("cannot schedule pod %s: %v", api.Key(pod.Namespace, pod.Name), err)
		return
	}

	nodeName, err := picker.Pick(pod, nodes, pods)
	if err != nil {
		log.Printf("cannot schedule pod %s: %v", api.Key(pod.Namespace, pod.Name), err)
		c.Recorder("scheduler").Warning("Pod", pod.Namespace, pod.Name, "FailedScheduling", "%v", err)
		return
	}

	err = c.BindPod(pod.Namespace, pod.Name, nodeName)
	if err != nil {
		log.Printf("cannot schedule pod %s: %v", api.Key(pod.Namespace, pod.Name), err)
		return
	}
	log.Printf("scheduled pod %s on node %q", api.Key(pod.Namespace, pod.Name), nodeName)
	c.Recorder("scheduler").Normal("Pod", pod.Namespace, pod.Name, "Scheduled", "assigned to node %q", nodeName)
}
