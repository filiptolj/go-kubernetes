package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// DeploymentController turns Deployments into ReplicaSets. Every version of a
// Deployment's pod template gets its own ReplicaSet. When the template
// changes, the controller moves pods from the old version to the new one
// step by step: a rolling update.
//
// During a rolling update there are never more than Replicas+1 pods, and
// never fewer than Replicas running and ready, so the app stays fully available.
type DeploymentController struct {
	Client *client.Client
	Every  time.Duration // how often to check
}

// Run reconciles every Deployment every dc.Every until ctx is cancelled.
func (dc *DeploymentController) Run(ctx context.Context) {
	ticker := time.NewTicker(dc.Every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dc.reconcileAll()
		}
	}
}

// reconcileAll moves every Deployment one step closer to how it should be,
// and removes ReplicaSets whose Deployment no longer exists.
func (dc *DeploymentController) reconcileAll() {
	deployments, err := dc.Client.ListDeployments("") // every namespace
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}

	sets, err := dc.Client.ListReplicaSets("")
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}

	pods, err := dc.Client.ListPods("")
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}

	// For every ReplicaSet, by its "namespace/name": how many of its pods are
	// alive, and how many are running and ready to serve.
	alive := make(map[string]int)
	running := make(map[string]int)
	for _, pod := range pods {
		owner := api.Key(pod.Namespace, pod.Owner)
		if isAlive(pod) {
			alive[owner]++
		}
		if pod.Phase == api.PodRunning && pod.Ready {
			running[owner]++
		}
	}

	// ReplicaSets, grouped by the "namespace/name" of their Deployment.
	owned := make(map[string][]api.ReplicaSet)
	for _, rs := range sets {
		if rs.Owner != "" {
			owner := api.Key(rs.Namespace, rs.Owner)
			owned[owner] = append(owned[owner], rs)
		}
	}

	exists := make(map[string]bool)
	for _, d := range deployments {
		key := api.Key(d.Namespace, d.Name)
		exists[key] = true
		dc.reconcile(d, owned[key], alive, running)
	}

	for owner, sets := range owned {
		if exists[owner] {
			continue
		}
		for _, rs := range sets {
			dc.deleteReplicaSet(rs, fmt.Sprintf("its deployment %q is gone", rs.Owner))
		}
	}
}

// reconcile moves one Deployment a single step closer to how it should be.
// Taking one small step per check is what makes the update gradual. alive and
// running count pods per ReplicaSet, by the ReplicaSet's "namespace/name".
func (dc *DeploymentController) reconcile(d api.Deployment, sets []api.ReplicaSet, alive, running map[string]int) {
	// count returns the pods of a ReplicaSet in one of the maps above.
	count := func(m map[string]int, rs api.ReplicaSet) int {
		return m[api.Key(rs.Namespace, rs.Name)]
	}

	newName := d.Name + "-" + templateHash(d.Template)

	var current *api.ReplicaSet
	var old []api.ReplicaSet
	for _, rs := range sets {
		if rs.Name == newName {
			current = &rs
		} else {
			old = append(old, rs)
		}
	}

	// A template we haven't seen: create its ReplicaSet. If older versions
	// exist, start it empty and grow it step by step.
	if current == nil {
		replicas := d.Replicas
		if len(old) > 0 {
			replicas = 0
			log.Printf("deployment %q: template changed, starting rolling update to %s", d.Name, newName)
			dc.events().Normal("Deployment", d.Namespace, d.Name, "RollingUpdate", "template changed, rolling out replicaset %q", newName)
		}
		dc.createReplicaSet(api.ReplicaSet{Name: newName, Namespace: d.Namespace, Replicas: replicas, Template: d.Template, Owner: d.Name})
		return
	}

	// No update in progress: just follow the Deployment's replica count.
	if len(old) == 0 {
		if current.Replicas != d.Replicas {
			dc.scale(*current, d.Replicas)
		}
		return
	}

	// A rolling update is in progress.
	if current.Replicas > d.Replicas {
		dc.scale(*current, d.Replicas) // the Deployment was scaled down meanwhile
		return
	}

	total := current.Replicas
	for _, rs := range old {
		total += rs.Replicas
	}

	// Step 1: add one pod of the new version, if we're not over the limit.
	if current.Replicas < d.Replicas && total < d.Replicas+1 {
		dc.scale(*current, current.Replicas+1)
		return
	}

	// Step 2: once enough pods run, remove one pod of an old version. Running
	// pods are only counted up to their ReplicaSet's size, because pods of a
	// ReplicaSet that was just scaled down may not have been deleted yet.
	runningTotal := min(count(running, *current), current.Replicas)
	for _, rs := range old {
		runningTotal += min(count(running, rs), rs.Replicas)
	}
	if runningTotal > d.Replicas {
		for _, rs := range old {
			if rs.Replicas > 0 {
				dc.scale(rs, rs.Replicas-1)
				return
			}
		}
	}

	// Step 3: delete old versions that have no pods left.
	for _, rs := range old {
		if rs.Replicas == 0 && count(alive, rs) == 0 {
			dc.deleteReplicaSet(rs, "the rolling update replaced it")
		}
	}
}

// templateHash returns a short fingerprint of a pod template. Identical
// templates always get the same hash; any change gives a different one.
func templateHash(t api.PodTemplate) string {
	data, _ := json.Marshal(t)

	h := fnv.New32a()
	h.Write(data)
	return fmt.Sprintf("%08x", h.Sum32())
}

// events returns the Deployment controller's event recorder.
func (dc *DeploymentController) events() *client.Recorder {
	return dc.Client.Recorder("deployment-controller")
}

func (dc *DeploymentController) createReplicaSet(rs api.ReplicaSet) {
	err := dc.Client.CreateReplicaSet(rs)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}
	log.Printf("deployment %s: created replicaset %q (%d replicas)", api.Key(rs.Namespace, rs.Owner), rs.Name, rs.Replicas)
	dc.events().Normal("Deployment", rs.Namespace, rs.Owner, "ScalingReplicaSet", "created replicaset %q with %d replicas", rs.Name, rs.Replicas)
}

func (dc *DeploymentController) scale(rs api.ReplicaSet, replicas int) {
	err := dc.Client.ScaleReplicaSet(rs.Namespace, rs.Name, replicas)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}
	log.Printf("deployment %s: scaled replicaset %q from %d to %d", api.Key(rs.Namespace, rs.Owner), rs.Name, rs.Replicas, replicas)
	dc.events().Normal("Deployment", rs.Namespace, rs.Owner, "ScalingReplicaSet", "scaled replicaset %q from %d to %d", rs.Name, rs.Replicas, replicas)
}

func (dc *DeploymentController) deleteReplicaSet(rs api.ReplicaSet, reason string) {
	err := dc.Client.DeleteReplicaSet(rs.Namespace, rs.Name)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}
	log.Printf("deleted replicaset %s: %s", api.Key(rs.Namespace, rs.Name), reason)
	dc.events().Normal("Deployment", rs.Namespace, rs.Owner, "DeletedReplicaSet", "deleted replicaset %q: %s", rs.Name, reason)
}
