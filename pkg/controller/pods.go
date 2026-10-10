package controller

import (
	"maps"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// templateHashLabel is the label that says which version of its template a
// pod was made from. DaemonSets and StatefulSets use it to find pods to
// replace after the template changes.
const templateHashLabel = "minik8s.template-hash"

// newPod returns a pod made from a template, owned by the object owner of
// the given kind.
func newPod(name string, t api.PodTemplateSpec, ownerKind string, owner api.ObjectMeta) api.Pod {
	// maps.Clone makes a copy. Without it, every pod would share the
	// template's one map, and adding a label to one pod would add it to all.
	labels := maps.Clone(t.Labels)
	if labels == nil {
		labels = make(api.Labels)
	}

	pod := api.Pod{
		TypeMeta:   api.TypeMetaFor("Pod"),
		ObjectMeta: api.ObjectMeta{Name: name, Namespace: owner.Namespace, Labels: labels},
		PodSpec:    t.PodSpec,
	}
	pod.SetOwner(ownerKind, owner.Name, owner.UID)
	return pod
}

// groupByOwner groups the pods created by objects of one kind, by the
// owner's "namespace/name".
func groupByOwner(pods []api.Pod, kind string) map[string][]api.Pod {
	owned := make(map[string][]api.Pod)
	for _, pod := range pods {
		if pod.ControlledBy(kind) {
			owner := api.Key(pod.Namespace, pod.OwnerName())
			owned[owner] = append(owned[owner], pod)
		}
	}
	return owned
}

// isRunningAndReady reports whether a pod runs and is ready for traffic.
func isRunningAndReady(pod api.Pod) bool {
	return pod.Phase == api.PodRunning && pod.Ready && !pod.Terminating()
}
