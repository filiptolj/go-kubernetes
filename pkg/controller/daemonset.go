package controller

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// DaemonSetController keeps one pod of every DaemonSet on every ready node.
// It places the pods itself, by creating them already bound to their node:
// the scheduler isn't involved.
type DaemonSetController struct {
	Client    *client.Client
	Informers *Informers    // where to read from; nil: from Client
	Every     time.Duration // how often to check even if nothing seems to change

	expected expectations // pods created or deleted but not seen yet, by DaemonSet
}

// Run checks the DaemonSets whenever one of them, a node or a pod changes,
// and every dc.Every, until ctx is cancelled.
func (dc *DaemonSetController) Run(ctx context.Context) {
	client.RunOnChange(ctx, "daemonset controller", dc.Every, dc.reconcileAll,
		dc.Informers.daemonSets(), dc.Informers.nodes(), dc.Informers.pods())
}

// reconcileAll checks every DaemonSet.
func (dc *DaemonSetController) reconcileAll() {
	dc.expected.check() // before reading: see expectations.go
	sets, err := list(dc.Informers.daemonSets(), dc.Client.DaemonSets().List)
	if err != nil {
		log.Printf("daemonset controller: %v", err)
		return
	}
	nodes, err := list(dc.Informers.nodes(), allNodes(dc.Client))
	if err != nil {
		log.Printf("daemonset controller: %v", err)
		return
	}
	pods, err := list(dc.Informers.pods(), dc.Client.ListPods)
	if err != nil {
		log.Printf("daemonset controller: %v", err)
		return
	}

	owned := groupByOwner(pods, "DaemonSet")
	for _, ds := range sets {
		key := api.Key(ds.Namespace, ds.Name)
		dc.reconcile(ds, owned[key], nodes)
	}

}

// reconcile makes sure every ready node runs exactly one pod of the
// DaemonSet, from the current version of its template.
func (dc *DaemonSetController) reconcile(ds api.DaemonSet, pods []api.Pod, nodes []api.Node) {
	if !dc.expected.satisfied(api.Key(ds.Namespace, ds.Name)) {
		return // the pods may not show our last changes yet
	}

	hash := templateHash(ds.Template)

	onNode := make(map[string][]api.Pod)
	leaving := make(map[string]bool) // nodes with a Terminating pod, whose name the new one needs
	for _, pod := range pods {
		if pod.Terminating() {
			leaving[pod.NodeName] = true
			continue
		}
		if !isAlive(pod) {
			dc.deletePod(pod, "it finished") // a fresh one takes its place
			continue
		}
		onNode[pod.NodeName] = append(onNode[pod.NodeName], pod)
	}

	ready := make(map[string]bool)
	for _, node := range nodes {
		if !node.Ready {
			continue
		}
		ready[node.Name] = true

		running := onNode[node.Name]
		if len(running) == 0 && !leaving[node.Name] {
			dc.createPod(ds, node.Name, hash)
		}
		for _, extra := range running[min(1, len(running)):] {
			dc.deletePod(extra, "there is more than one on its node")
		}
	}

	// Pods on nodes that were removed have nowhere to run. (Pods on nodes that
	// are only NotReady are failed by the node controller, then replaced.)
	allNodes := make(map[string]bool)
	for _, node := range nodes {
		allNodes[node.Name] = true
	}
	for nodeName, running := range onNode {
		if !allNodes[nodeName] {
			for _, pod := range running {
				dc.deletePod(pod, fmt.Sprintf("its node %q is gone", nodeName))
			}
		}
	}

	// One pod at a time: while one is still leaving, wait.
	if len(leaving) > 0 {
		return
	}

	// A rolling update: once every node's pod is ready, replace one pod that
	// runs an older version of the template. Its replacement is created on
	// the next check, and the next one is replaced only once it is ready.
	for nodeName := range ready {
		for _, pod := range onNode[nodeName] {
			if !isRunningAndReady(pod) {
				return
			}
		}
	}
	for nodeName := range ready {
		for _, pod := range onNode[nodeName] {
			if pod.Labels[templateHashLabel] != hash {
				dc.deletePod(pod, "its template changed")
				return
			}
		}
	}
}

// createPod creates the DaemonSet's pod for one node, already bound to it.
func (dc *DaemonSetController) createPod(ds api.DaemonSet, nodeName, hash string) {
	pod := newPod(ds.Name+"-"+nodeName, ds.Template, "DaemonSet", ds.ObjectMeta)
	pod.Labels[templateHashLabel] = hash
	pod.NodeName = nodeName

	err := dc.Client.CreatePod(pod)
	if err != nil {
		log.Printf("daemonset %s: %v", api.Key(ds.Namespace, ds.Name), err)
		return
	}
	expectPresent(&dc.expected, dc.Informers.pods(), api.Key(ds.Namespace, ds.Name), pod.Namespace, pod.Name)
	log.Printf("daemonset %s: created pod %q on node %q", api.Key(ds.Namespace, ds.Name), pod.Name, nodeName)
	dc.events().Normal("DaemonSet", ds.Namespace, ds.Name, "SuccessfulCreate", "created pod %q on node %q", pod.Name, nodeName)
}

func (dc *DaemonSetController) deletePod(pod api.Pod, reason string) {
	err := dc.Client.DeletePod(pod.Namespace, pod.Name)
	if err != nil {
		log.Printf("daemonset controller: %v", err)
		return
	}
	expectGone(&dc.expected, dc.Informers.pods(), api.Key(pod.Namespace, pod.OwnerName()), pod.Namespace, pod.Name)
	log.Printf("deleted pod %s: %s", api.Key(pod.Namespace, pod.Name), reason)
	dc.events().Normal("DaemonSet", pod.Namespace, pod.OwnerName(), "SuccessfulDelete", "deleted pod %q: %s", pod.Name, reason)
}

// events returns the DaemonSet controller's event recorder.
func (dc *DaemonSetController) events() *client.Recorder {
	return dc.Client.Recorder("daemonset-controller")
}
