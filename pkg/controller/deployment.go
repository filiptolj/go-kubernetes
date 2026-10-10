package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"maps"
	"slices"
	"strconv"
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
	Client    *client.Client
	Informers *Informers    // where to read from; nil: from Client
	Every     time.Duration // how often to check even if nothing seems to change

	expected expectations // changes to ReplicaSets not seen yet, by Deployment
}

// Run reconciles every Deployment whenever a Deployment, a ReplicaSet or a
// pod changes, and every dc.Every, until ctx is cancelled.
func (dc *DeploymentController) Run(ctx context.Context) {
	client.RunOnChange(ctx, "deployment controller", dc.Every, dc.reconcileAll,
		dc.Informers.deployments(), dc.Informers.replicaSets(), dc.Informers.pods())
}

// reconcileAll moves every Deployment one step closer to how it should be.
func (dc *DeploymentController) reconcileAll() {
	dc.expected.check() // before reading: see expectations.go
	deployments, err := list(dc.Informers.deployments(), dc.Client.ListDeployments)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}

	sets, err := list(dc.Informers.replicaSets(), dc.Client.ListReplicaSets)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}

	pods, err := list(dc.Informers.pods(), dc.Client.ListPods)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}

	// For every ReplicaSet, by its "namespace/name": how many of its pods are
	// alive, and how many are running and ready to serve.
	alive := make(map[string]int)
	running := make(map[string]int)
	for _, pod := range pods {
		if !pod.ControlledBy("ReplicaSet") {
			continue
		}
		owner := api.Key(pod.Namespace, pod.OwnerName())
		if isAlive(pod) {
			alive[owner]++
		}
		if isRunningAndReady(pod) {
			running[owner]++
		}
	}

	// ReplicaSets, grouped by the "namespace/name" of their Deployment.
	owned := make(map[string][]api.ReplicaSet)
	for _, rs := range sets {
		if rs.ControlledBy("Deployment") {
			owner := api.Key(rs.Namespace, rs.OwnerName())
			owned[owner] = append(owned[owner], rs)
		}
	}

	for _, d := range deployments {
		key := api.Key(d.Namespace, d.Name)
		dc.reconcile(d, owned[key], alive, running)
	}

}

// reconcile moves one Deployment a single step closer to how it should be.
// Taking one small step per check is what makes the update gradual. alive and
// running count pods per ReplicaSet, by the ReplicaSet's "namespace/name".
func (dc *DeploymentController) reconcile(d api.Deployment, sets []api.ReplicaSet, alive, running map[string]int) {
	if !dc.expected.satisfied(api.Key(d.Namespace, d.Name)) {
		return // the ReplicaSets may not show our last step yet
	}

	// count returns the pods of a ReplicaSet in one of the maps above.
	count := func(m map[string]int, rs api.ReplicaSet) int {
		return m[api.Key(rs.Namespace, rs.Name)]
	}

	newName := d.Name + "-" + templateHash(d.Template)

	var current *api.ReplicaSet
	var old []api.ReplicaSet
	newest := 0 // the highest revision so far
	for _, rs := range sets {
		newest = max(newest, revision(rs))
		if rs.Name == newName {
			current = &rs
		} else {
			old = append(old, rs)
		}
	}

	// The old versions that still have pods, or are meant to. Old versions
	// scaled down to nothing are kept, so `rollout undo` can go back to them.
	var active []api.ReplicaSet
	for _, rs := range old {
		if rs.Replicas > 0 || count(alive, rs) > 0 {
			active = append(active, rs)
		}
	}

	// A template we haven't seen: create its ReplicaSet, as the newest
	// revision. If older versions are running, start it empty and grow it
	// step by step.
	if current == nil {
		replicas := d.Replicas
		if len(active) > 0 {
			replicas = 0
			log.Printf("deployment %q: template changed, starting rolling update to %s", d.Name, newName)
			dc.events().Normal("Deployment", d.Namespace, d.Name, "RollingUpdate", "template changed, rolling out replicaset %q", newName)
		}
		rs := api.ReplicaSet{
			TypeMeta: api.TypeMetaFor("ReplicaSet"),
			ObjectMeta: api.ObjectMeta{Name: newName, Namespace: d.Namespace,
				Annotations: map[string]string{api.RevisionAnnotation: strconv.Itoa(newest + 1)}},
			ReplicaSetSpec: api.ReplicaSetSpec{Replicas: replicas, Template: d.Template},
		}
		rs.SetOwner("Deployment", d.Name, d.UID)
		dc.createReplicaSet(rs)
		return
	}

	// The template is that of an older version again, after `rollout undo`
	// or an edit back: that version becomes the newest revision.
	if revision(*current) < newest {
		dc.renumber(*current, newest+1)
		return
	}

	// No update in progress: follow the Deployment's replica count, and
	// forget versions beyond the history limit.
	if len(active) == 0 {
		if current.Replicas != d.Replicas {
			dc.scale(*current, d.Replicas)
		}
		dc.pruneHistory(d, old)
		return
	}

	// A rolling update is in progress.
	if current.Replicas > d.Replicas {
		dc.scale(*current, d.Replicas) // the Deployment was scaled down meanwhile
		return
	}

	total := current.Replicas
	for _, rs := range active {
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
	for _, rs := range active {
		runningTotal += min(count(running, rs), rs.Replicas)
	}
	if runningTotal > d.Replicas {
		for _, rs := range active {
			if rs.Replicas > 0 {
				dc.scale(rs, rs.Replicas-1)
				return
			}
		}
	}
}

// revision returns the revision of one of a Deployment's ReplicaSets, or 0
// if it has none.
func revision(rs api.ReplicaSet) int {
	n, _ := strconv.Atoi(rs.Annotations[api.RevisionAnnotation])
	return n
}

// renumber gives a ReplicaSet a new revision.
func (dc *DeploymentController) renumber(rs api.ReplicaSet, rev int) {
	old := rs.ResourceVersion
	rs.Annotations = maps.Clone(rs.Annotations)
	if rs.Annotations == nil {
		rs.Annotations = make(map[string]string)
	}
	rs.Annotations[api.RevisionAnnotation] = strconv.Itoa(rev)

	err := dc.Client.UpdateReplicaSet(rs)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}
	expectNewVersion(&dc.expected, dc.Informers.replicaSets(), api.Key(rs.Namespace, rs.OwnerName()), rs.Namespace, rs.Name, old)
	log.Printf("deployment %s: rolling back to replicaset %q, now revision %d", api.Key(rs.Namespace, rs.OwnerName()), rs.Name, rev)
	dc.events().Normal("Deployment", rs.Namespace, rs.OwnerName(), "RollingUpdate", "rolling out replicaset %q again, as revision %d", rs.Name, rev)
}

// pruneHistory deletes a Deployment's oldest unused ReplicaSets, beyond its
// revisionHistoryLimit.
func (dc *DeploymentController) pruneHistory(d api.Deployment, old []api.ReplicaSet) {
	limit := 10
	if d.RevisionHistoryLimit != nil {
		limit = *d.RevisionHistoryLimit
	}
	if len(old) <= limit {
		return
	}
	slices.SortFunc(old, func(a, b api.ReplicaSet) int { return revision(a) - revision(b) })
	for _, rs := range old[:len(old)-limit] {
		dc.deleteReplicaSet(rs, fmt.Sprintf("it is older than the %d revisions the deployment keeps", limit))
	}
}

// templateHash returns a short fingerprint of a pod template. Identical
// templates always get the same hash; any change gives a different one.
func templateHash(t api.PodTemplateSpec) string {
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
	expectPresent(&dc.expected, dc.Informers.replicaSets(), api.Key(rs.Namespace, rs.OwnerName()), rs.Namespace, rs.Name)
	log.Printf("deployment %s: created replicaset %q (%d replicas)", api.Key(rs.Namespace, rs.OwnerName()), rs.Name, rs.Replicas)
	dc.events().Normal("Deployment", rs.Namespace, rs.OwnerName(), "ScalingReplicaSet", "created replicaset %q with %d replicas", rs.Name, rs.Replicas)
}

func (dc *DeploymentController) scale(rs api.ReplicaSet, replicas int) {
	err := dc.Client.ScaleReplicaSet(rs.Namespace, rs.Name, replicas)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}
	expectNewVersion(&dc.expected, dc.Informers.replicaSets(), api.Key(rs.Namespace, rs.OwnerName()), rs.Namespace, rs.Name, rs.ResourceVersion)
	log.Printf("deployment %s: scaled replicaset %q from %d to %d", api.Key(rs.Namespace, rs.OwnerName()), rs.Name, rs.Replicas, replicas)
	dc.events().Normal("Deployment", rs.Namespace, rs.OwnerName(), "ScalingReplicaSet", "scaled replicaset %q from %d to %d", rs.Name, rs.Replicas, replicas)
}

func (dc *DeploymentController) deleteReplicaSet(rs api.ReplicaSet, reason string) {
	err := dc.Client.DeleteReplicaSet(rs.Namespace, rs.Name)
	if err != nil {
		log.Printf("deployment controller: %v", err)
		return
	}
	expectGone(&dc.expected, dc.Informers.replicaSets(), api.Key(rs.Namespace, rs.OwnerName()), rs.Namespace, rs.Name)
	log.Printf("deleted replicaset %s: %s", api.Key(rs.Namespace, rs.Name), reason)
	dc.events().Normal("Deployment", rs.Namespace, rs.OwnerName(), "DeletedReplicaSet", "deleted replicaset %q: %s", rs.Name, reason)
}
