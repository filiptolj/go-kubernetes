// Package controller holds the reconcile loops that keep the cluster healthy.
// Each one compares how things should be with how they are, and fixes the
// difference.
package controller

import (
	"context"
	"log"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// NodeController watches node heartbeats. When a kubelet goes quiet for longer
// than Timeout, it marks the node NotReady and fails the pods on it, so they
// can be replaced on another node.
type NodeController struct {
	Client    *client.Client
	Informers *Informers    // where to read from; nil: from Client
	Timeout   time.Duration // how long a node may go without a heartbeat
	Every     time.Duration // how often to check
}

// Run checks the nodes every nc.Every until ctx is cancelled. Missing
// heartbeats are about time passing, not about something changing, so it
// doesn't react to changes; it only waits for its informers to fill first.
func (nc *NodeController) Run(ctx context.Context) {
	if !nc.Informers.nodes().WaitForSync(ctx) || !nc.Informers.pods().WaitForSync(ctx) {
		return
	}

	ticker := time.NewTicker(nc.Every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nc.reconcile()
		}
	}
}

// reconcile marks silent nodes NotReady and fails the live pods on NotReady nodes.
func (nc *NodeController) reconcile() {
	nodes, err := list(nc.Informers.nodes(), allNodes(nc.Client))
	if err != nil {
		log.Printf("node controller: %v", err)
		return
	}

	pods, err := list(nc.Informers.pods(), nc.Client.ListPods)
	if err != nil {
		log.Printf("node controller: %v", err)
		return
	}

	for _, node := range nodes {
		// Nodes created with apply have no kubelet, so they never send
		// heartbeats. Leave them alone.
		if node.LastHeartbeat.IsZero() {
			continue
		}

		silence := time.Since(node.LastHeartbeat)
		if node.Ready && silence > nc.Timeout {
			log.Printf("node %q: no heartbeat for %s, marking it NotReady", node.Name, silence.Round(time.Second))
			nc.events().Warning("Node", "", node.Name, "NodeNotReady", "no heartbeat from its kubelet for %s", silence.Round(time.Second))
			err := nc.Client.SetNodeReady(node.Name, false)
			if err != nil {
				log.Printf("node controller: %v", err)
				continue
			}
			node.Ready = false
		}

		if node.Ready {
			continue
		}

		for _, pod := range pods {
			if pod.NodeName != node.Name || !isAlive(pod) {
				continue
			}
			log.Printf("node %q is NotReady: failing pod %s", node.Name, api.Key(pod.Namespace, pod.Name))
			nc.events().Warning("Pod", pod.Namespace, pod.Name, "NodeLost", "node %q stopped responding", node.Name)
			err := nc.Client.SetPodPhase(pod.Namespace, pod.Name, api.PodFailed)
			if err != nil {
				log.Printf("node controller: %v", err)
			}
		}
	}
}

// events returns the node controller's event recorder.
func (nc *NodeController) events() *client.Recorder {
	return nc.Client.Recorder("node-controller")
}

// isAlive reports whether a pod is still waiting to run or running.
func isAlive(pod api.Pod) bool {
	return pod.Phase == api.PodPending || pod.Phase == api.PodRunning
}
