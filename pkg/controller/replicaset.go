package controller

import (
	"context"
	"log"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// ReplicaSetController makes sure every ReplicaSet has exactly as many live
// pods as it asks for.
//
// When a ReplicaSet's pods keep failing, it waits longer and longer before
// replacing them: BackoffBase after the first failure, then twice that, and
// so on up to BackoffMax. A pod that ran for at least HealthyAfter before
// failing resets the count. This is a crash-loop backoff.
type ReplicaSetController struct {
	Client    *client.Client
	Informers *Informers    // where to read pods and ReplicaSets; nil: from Client
	Resync    time.Duration // how often to check even if nothing seems to change

	BackoffBase  time.Duration
	BackoffMax   time.Duration
	HealthyAfter time.Duration

	backoff  map[string]*backoff // by the ReplicaSet's "namespace/name"
	expected expectations        // pods created or deleted but not seen yet
}

// backoff tracks how a ReplicaSet's pods have been failing.
type backoff struct {
	failures int       // failed pods in a row
	until    time.Time // don't create pods before this
	logged   bool      // whether we already said we're waiting
}

// Run reconciles every ReplicaSet whenever a pod or a ReplicaSet changes,
// and also every Resync interval, until ctx is cancelled.
func (rc *ReplicaSetController) Run(ctx context.Context) {
	client.RunOnChange(ctx, "replicaset controller", rc.Resync, rc.reconcileAll,
		rc.Informers.pods(), rc.Informers.replicaSets())
}

// reconcileAll compares every ReplicaSet with its pods and fixes any
// difference.
func (rc *ReplicaSetController) reconcileAll() {
	rc.expected.check() // before reading: see expectations.go
	sets, err := list(rc.Informers.replicaSets(), rc.Client.ListReplicaSets)
	if err != nil {
		log.Printf("replicaset controller: %v", err)
		return
	}

	pods, err := list(rc.Informers.pods(), rc.Client.ListPods)
	if err != nil {
		log.Printf("replicaset controller: %v", err)
		return
	}

	// Pods, grouped by the "namespace/name" of the ReplicaSet that owns them.
	// A ReplicaSet only owns pods in its own namespace.
	owned := make(map[string][]api.Pod)
	for _, pod := range pods {
		if pod.ControlledBy("ReplicaSet") {
			owner := api.Key(pod.Namespace, pod.OwnerName())
			owned[owner] = append(owned[owner], pod)
		}
	}

	exists := make(map[string]bool)
	for _, rs := range sets {
		key := api.Key(rs.Namespace, rs.Name)
		exists[key] = true
		rc.reconcile(rs, owned[key])
	}

	for key := range rc.backoff {
		if !exists[key] {
			delete(rc.backoff, key)
		}
	}

}

// reconcile makes one ReplicaSet's pods match the number of replicas it wants.
func (rc *ReplicaSetController) reconcile(rs api.ReplicaSet, pods []api.Pod) {
	if !rc.expected.satisfied(api.Key(rs.Namespace, rs.Name)) {
		return // the pods may not show our last changes yet
	}

	var alive []api.Pod
	for _, pod := range pods {
		if pod.Terminating() {
			continue // on its way out: neither counted nor deleted again
		}
		if isAlive(pod) {
			alive = append(alive, pod)
		} else {
			// Finished pods don't count. Clear them away; a fresh pod takes their place.
			if pod.Phase == api.PodFailed {
				rc.recordFailure(rs, pod)
			}
			rc.deletePod(pod, "it "+string(pod.Phase))
		}
	}

	missing := rs.Replicas - len(alive)
	switch {
	case missing > 0:
		if rc.backingOff(rs) {
			return
		}
		for range missing {
			rc.createPod(rs)
		}
	case missing < 0:
		// Remove pods that aren't running yet first: they are the cheapest to lose.
		sort.SliceStable(alive, func(i, j int) bool {
			return alive[i].Phase == api.PodPending && alive[j].Phase != api.PodPending
		})
		for _, pod := range alive[:-missing] {
			rc.deletePod(pod, "there are too many replicas")
		}
	}
}

// recordFailure counts a failed pod of a ReplicaSet and works out how long to
// wait before replacing it.
func (rc *ReplicaSetController) recordFailure(rs api.ReplicaSet, pod api.Pod) {
	if rc.backoff == nil {
		rc.backoff = make(map[string]*backoff)
	}
	key := api.Key(rs.Namespace, rs.Name)
	b, ok := rc.backoff[key]
	if !ok {
		b = &backoff{}
		rc.backoff[key] = b
	}

	// A pod that ran fine for a while before failing isn't crash-looping:
	// start counting from scratch.
	ranFor := pod.FinishedAt.Sub(pod.StartedAt)
	if !pod.StartedAt.IsZero() && ranFor >= rc.HealthyAfter {
		b.failures = 0
	}
	b.failures++

	delay := rc.BackoffBase
	for range b.failures - 1 {
		delay *= 2
		if delay >= rc.BackoffMax {
			delay = rc.BackoffMax
			break
		}
	}

	b.until = time.Now().Add(delay)
	b.logged = false
	if delay > 0 {
		log.Printf("replicaset %s: pod %q failed (%d in a row), waiting %s before replacing it",
			key, pod.Name, b.failures, delay)
		rc.events().Warning("ReplicaSet", rs.Namespace, rs.Name, "BackOff", "pod %q failed (%d in a row), waiting %s before replacing it",
			pod.Name, b.failures, delay)
	}
}

// backingOff reports whether the ReplicaSet must still wait before creating pods.
func (rc *ReplicaSetController) backingOff(rs api.ReplicaSet) bool {
	key := api.Key(rs.Namespace, rs.Name)
	b, ok := rc.backoff[key]
	if !ok || time.Now().After(b.until) {
		return false
	}

	if !b.logged {
		log.Printf("replicaset %s: backing off until %s", key, b.until.Format(time.TimeOnly))
		b.logged = true
	}
	return true
}

// createPod creates one new pod from the ReplicaSet's template.
func (rc *ReplicaSetController) createPod(rs api.ReplicaSet) {
	pod := newPod(newPodName(rs.Name), rs.Template, "ReplicaSet", rs.ObjectMeta)

	err := rc.Client.CreatePod(pod)
	if err != nil {
		log.Printf("replicaset %s: %v", api.Key(rs.Namespace, rs.Name), err)
		return
	}
	expectPresent(&rc.expected, rc.Informers.pods(), api.Key(rs.Namespace, rs.Name), pod.Namespace, pod.Name)
	log.Printf("replicaset %s: created pod %q", api.Key(rs.Namespace, rs.Name), pod.Name)
	rc.events().Normal("ReplicaSet", rs.Namespace, rs.Name, "SuccessfulCreate", "created pod %q", pod.Name)
}

// deletePod deletes a pod and logs why.
func (rc *ReplicaSetController) deletePod(pod api.Pod, reason string) {
	err := rc.Client.DeletePod(pod.Namespace, pod.Name)
	if err != nil {
		log.Printf("replicaset controller: %v", err)
		return
	}
	expectGone(&rc.expected, rc.Informers.pods(), api.Key(pod.Namespace, pod.OwnerName()), pod.Namespace, pod.Name)
	log.Printf("deleted pod %s: %s", api.Key(pod.Namespace, pod.Name), reason)
	if pod.ControlledBy("ReplicaSet") {
		rc.events().Normal("ReplicaSet", pod.Namespace, pod.OwnerName(), "SuccessfulDelete", "deleted pod %q: %s", pod.Name, reason)
	}
}

// events returns the ReplicaSet controller's event recorder.
func (rc *ReplicaSetController) events() *client.Recorder {
	return rc.Client.Recorder("replicaset-controller")
}

// newPodName returns a name for a new pod of a ReplicaSet, like "web-x7k2p".
func newPodName(rsName string) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"

	suffix := make([]byte, 5)
	for i := range suffix {
		suffix[i] = letters[rand.IntN(len(letters))]
	}
	return rsName + "-" + string(suffix)
}
