package controller

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// StatefulSetController runs the numbered pods of every StatefulSet. Pods
// are created in order (name-0, name-1, ...), each only once the one before
// it runs and is ready; they are removed in reverse order, one at a time; a
// pod that is gone is created again with the same name.
type StatefulSetController struct {
	Client *client.Client
	Every  time.Duration // how often to check
}

// Run checks the StatefulSets every sc.Every until ctx is cancelled.
func (sc *StatefulSetController) Run(ctx context.Context) {
	runEvery(ctx, sc.Every, sc.reconcileAll)
}

// reconcileAll checks every StatefulSet, and removes pods whose StatefulSet is gone.
func (sc *StatefulSetController) reconcileAll() {
	sets, err := sc.Client.StatefulSets().List("")
	if err != nil {
		log.Printf("statefulset controller: %v", err)
		return
	}
	pods, err := sc.Client.ListPods("")
	if err != nil {
		log.Printf("statefulset controller: %v", err)
		return
	}

	owned := groupByOwner(pods, "StatefulSet")
	exists := make(map[string]bool)
	for _, ss := range sets {
		key := api.Key(ss.Namespace, ss.Name)
		exists[key] = true
		sc.reconcile(ss, owned[key])
	}

	for owner, pods := range owned {
		if exists[owner] {
			continue
		}
		for _, pod := range pods {
			sc.deletePod(pod, fmt.Sprintf("its statefulset %q is gone", pod.Owner))
		}
	}
}

// ordinal returns the number at the end of a StatefulSet pod's name: 2 for
// "db-2". ok is false if the name doesn't have the StatefulSet's form.
func ordinal(ss api.StatefulSet, pod api.Pod) (n int, ok bool) {
	suffix, found := strings.CutPrefix(pod.Name, ss.Name+"-")
	if !found {
		return 0, false
	}
	n, err := strconv.Atoi(suffix)
	return n, err == nil && n >= 0
}

// reconcile takes one step towards the StatefulSet's desired state. Doing a
// single step per check is what makes it go one pod at a time, in order.
func (sc *StatefulSetController) reconcile(ss api.StatefulSet, pods []api.Pod) {
	hash := templateHash(ss.Template)

	byOrdinal := make(map[int]api.Pod)
	highest := -1
	for _, pod := range pods {
		n, ok := ordinal(ss, pod)
		if !ok {
			continue
		}
		if !isAlive(pod) {
			// A finished pod is deleted, and created again with the same name.
			sc.deletePod(pod, "it finished")
			continue
		}
		byOrdinal[n] = pod
		highest = max(highest, n)
	}

	// Too many: remove the highest-numbered pod first.
	if highest >= ss.Replicas {
		sc.deletePod(byOrdinal[highest], "the statefulset was scaled down")
		return
	}

	// Create the first missing pod, but only once every pod before it runs
	// and is ready.
	for i := range ss.Replicas {
		pod, ok := byOrdinal[i]
		if !ok {
			sc.createPod(ss, i, hash)
			return
		}
		if !isRunningAndReady(pod) {
			return // wait for it
		}
	}

	// Every pod runs and is ready. If the template changed, replace the
	// highest-numbered pod that runs an older version; the next ones follow,
	// one at a time, once each replacement is ready.
	for i := ss.Replicas - 1; i >= 0; i-- {
		if byOrdinal[i].Labels[templateHashLabel] != hash {
			sc.deletePod(byOrdinal[i], "its template changed")
			return
		}
	}
}

func (sc *StatefulSetController) createPod(ss api.StatefulSet, n int, hash string) {
	pod := newPod(fmt.Sprintf("%s-%d", ss.Name, n), ss.Namespace, ss.Template, "StatefulSet", ss.Name)
	pod.Labels[templateHashLabel] = hash

	err := sc.Client.CreatePod(pod)
	if err != nil {
		log.Printf("statefulset %s: %v", api.Key(ss.Namespace, ss.Name), err)
		return
	}
	log.Printf("statefulset %s: created pod %q", api.Key(ss.Namespace, ss.Name), pod.Name)
	sc.events().Normal("StatefulSet", ss.Namespace, ss.Name, "SuccessfulCreate", "created pod %q", pod.Name)
}

func (sc *StatefulSetController) deletePod(pod api.Pod, reason string) {
	err := sc.Client.DeletePod(pod.Namespace, pod.Name)
	if err != nil {
		log.Printf("statefulset controller: %v", err)
		return
	}
	log.Printf("deleted pod %s: %s", api.Key(pod.Namespace, pod.Name), reason)
	sc.events().Normal("StatefulSet", pod.Namespace, pod.Owner, "SuccessfulDelete", "deleted pod %q: %s", pod.Name, reason)
}

// events returns the StatefulSet controller's event recorder.
func (sc *StatefulSetController) events() *client.Recorder {
	return sc.Client.Recorder("statefulset-controller")
}
