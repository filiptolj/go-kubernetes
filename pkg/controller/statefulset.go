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
	Client    *client.Client
	Informers *Informers    // where to read from; nil: from Client
	Every     time.Duration // how often to check even if nothing seems to change

	expected expectations // pods created or deleted but not seen yet, by StatefulSet
}

// Run checks the StatefulSets whenever one of them or a pod changes, and
// every sc.Every, until ctx is cancelled.
func (sc *StatefulSetController) Run(ctx context.Context) {
	client.RunOnChange(ctx, "statefulset controller", sc.Every, sc.reconcileAll,
		sc.Informers.statefulSets(), sc.Informers.pods())
}

// reconcileAll checks every StatefulSet.
func (sc *StatefulSetController) reconcileAll() {
	sc.expected.check() // before reading: see expectations.go
	sets, err := list(sc.Informers.statefulSets(), sc.Client.StatefulSets().List)
	if err != nil {
		log.Printf("statefulset controller: %v", err)
		return
	}
	pods, err := list(sc.Informers.pods(), sc.Client.ListPods)
	if err != nil {
		log.Printf("statefulset controller: %v", err)
		return
	}

	owned := groupByOwner(pods, "StatefulSet")
	for _, ss := range sets {
		key := api.Key(ss.Namespace, ss.Name)
		sc.reconcile(ss, owned[key])
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
	if !sc.expected.satisfied(api.Key(ss.Namespace, ss.Name)) {
		return // the pods may not show our last step yet
	}

	hash := templateHash(ss.Template)

	byOrdinal := make(map[int]api.Pod)
	highest := -1
	for _, pod := range pods {
		n, ok := ordinal(ss, pod)
		if !ok {
			continue
		}
		if pod.Terminating() {
			// Going one step at a time includes waiting for this one to be
			// gone: its replacement will need its name.
			return
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
	pod := newPod(fmt.Sprintf("%s-%d", ss.Name, n), ss.Template, "StatefulSet", ss.ObjectMeta)
	pod.Labels[templateHashLabel] = hash

	err := sc.Client.CreatePod(pod)
	if err != nil {
		log.Printf("statefulset %s: %v", api.Key(ss.Namespace, ss.Name), err)
		return
	}
	expectPresent(&sc.expected, sc.Informers.pods(), api.Key(ss.Namespace, ss.Name), pod.Namespace, pod.Name)
	log.Printf("statefulset %s: created pod %q", api.Key(ss.Namespace, ss.Name), pod.Name)
	sc.events().Normal("StatefulSet", ss.Namespace, ss.Name, "SuccessfulCreate", "created pod %q", pod.Name)
}

func (sc *StatefulSetController) deletePod(pod api.Pod, reason string) {
	err := sc.Client.DeletePod(pod.Namespace, pod.Name)
	if err != nil {
		log.Printf("statefulset controller: %v", err)
		return
	}
	expectGone(&sc.expected, sc.Informers.pods(), api.Key(pod.Namespace, pod.OwnerName()), pod.Namespace, pod.Name)
	log.Printf("deleted pod %s: %s", api.Key(pod.Namespace, pod.Name), reason)
	sc.events().Normal("StatefulSet", pod.Namespace, pod.OwnerName(), "SuccessfulDelete", "deleted pod %q: %s", pod.Name, reason)
}

// events returns the StatefulSet controller's event recorder.
func (sc *StatefulSetController) events() *client.Recorder {
	return sc.Client.Recorder("statefulset-controller")
}
