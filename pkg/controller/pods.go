package controller

import (
	"maps"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// templateHashLabel is the label that says which version of its template a
// pod was made from. DaemonSets and StatefulSets use it to find pods to
// replace after the template changes.
const templateHashLabel = "minik8s.template-hash"

// newPod returns a pod made from a template, owned by the object of the
// given kind and name.
func newPod(name, namespace string, t api.PodTemplate, ownerKind, owner string) api.Pod {
	// maps.Clone makes a copy. Without it, every pod would share the
	// template's one map, and adding a label to one pod would add it to all.
	labels := maps.Clone(t.Labels)
	if labels == nil {
		labels = make(api.Labels)
	}

	return api.Pod{
		Name:          name,
		Namespace:     namespace,
		Labels:        labels,
		Containers:    t.Containers,
		Volumes:       t.Volumes,
		RestartPolicy: t.RestartPolicy,
		Owner:         owner,
		OwnerKind:     ownerKind,
	}
}

// groupByOwner groups the pods created by objects of one kind, by the
// owner's "namespace/name".
func groupByOwner(pods []api.Pod, kind string) map[string][]api.Pod {
	owned := make(map[string][]api.Pod)
	for _, pod := range pods {
		if pod.ControlledBy(kind) {
			owner := api.Key(pod.Namespace, pod.Owner)
			owned[owner] = append(owned[owner], pod)
		}
	}
	return owned
}

// isRunningAndReady reports whether a pod runs and is ready for traffic.
func isRunningAndReady(pod api.Pod) bool {
	return pod.Phase == api.PodRunning && pod.Ready
}
