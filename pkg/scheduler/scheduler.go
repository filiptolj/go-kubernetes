package scheduler

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// Scheduler binds every pending pod to a node. It reads pods and nodes from
// informers, and looks again whenever they change.
type Scheduler struct {
	Client *client.Client
	Picker Picker
	Pods   *client.Informer[api.Pod]
	Nodes  *client.Informer[api.Node]
	Resync time.Duration // how often to retry pods that can't be scheduled

	// assumed remembers the pods this scheduler has bound but that the
	// informer doesn't show as bound yet, by "namespace/name". Without it,
	// a burst of new pods would all see the same, outdated load on the
	// nodes, and all go to the same one. Kubernetes' scheduler does the same.
	assumed map[string]string

	// failed remembers when each pod last failed to be scheduled, so that a
	// pod waiting for a node doesn't log the same failure on every change.
	failed map[string]time.Time
}

// Run schedules pods until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	client.RunOnChange(ctx, "scheduler", s.Resync, s.scheduleAll, s.Pods, s.Nodes)
}

// scheduleAll schedules every pod that is waiting for a node.
func (s *Scheduler) scheduleAll() {
	if s.assumed == nil {
		s.assumed = make(map[string]string)
		s.failed = make(map[string]time.Time)
	}

	pods := s.Pods.List("") // a node's load counts pods from every namespace
	nodes := s.Nodes.List("")

	// Put in what the informer doesn't show yet: the pods we have bound. An
	// assumption ends once the informer shows the pod bound, or gone.
	stillAssumed := make(map[string]string)
	for i, pod := range pods {
		key := api.Key(pod.Namespace, pod.Name)
		if node, ok := s.assumed[key]; ok && pod.NodeName == "" {
			pods[i].NodeName = node
			stillAssumed[key] = node
		}
	}
	s.assumed = stillAssumed

	for i, pod := range pods {
		if pod.NodeName != "" || pod.Phase != api.PodPending {
			continue
		}
		node, ok := s.schedule(pod, nodes, pods)
		if ok {
			pods[i].NodeName = node // so the next pod sees this one's load
		}
	}
}

// schedule picks a node for one pod and binds the pod to it. It returns the
// node, and whether the pod was bound.
func (s *Scheduler) schedule(pod api.Pod, nodes []api.Node, pods []api.Pod) (string, bool) {
	key := api.Key(pod.Namespace, pod.Name)

	candidates, err := fit(pod, nodes, pods)
	var nodeName string
	if err == nil {
		nodeName, err = s.Picker.Pick(pod, candidates, pods)
	}
	if err != nil {
		if time.Since(s.failed[key]) >= s.Resync {
			s.failed[key] = time.Now()
			log.Printf("cannot schedule pod %s: %v", key, err)
			s.Client.Recorder("scheduler").Warning("Pod", pod.Namespace, pod.Name, "FailedScheduling", "%v", err)
		}
		return "", false
	}
	delete(s.failed, key)

	err = s.Client.BindPod(pod.Namespace, pod.Name, nodeName)
	if errors.Is(err, client.ErrConflict) {
		return "", false // someone bound it already; the informer will show it
	}
	if err != nil {
		log.Printf("cannot schedule pod %s: %v", key, err)
		return "", false
	}

	s.assumed[key] = nodeName
	log.Printf("scheduled pod %s on node %q", key, nodeName)
	s.Client.Recorder("scheduler").Normal("Pod", pod.Namespace, pod.Name, "Scheduled", "assigned to node %q", nodeName)
	return nodeName, true
}
