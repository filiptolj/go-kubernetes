package scheduler

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// fit returns the nodes pod can go to: ready, and with enough CPU and
// memory left that the pods already there haven't requested. If there are
// none, the error says what was wrong with each node, the way Kubernetes
// does: "0/3 nodes are available: 1 not ready, 2 too little memory".
func fit(pod api.Pod, nodes []api.Node, pods []api.Pod) ([]api.Node, error) {
	if len(nodes) == 0 {
		return nil, ErrNoNodes
	}

	// What the pods on each node have requested. Finished pods don't count:
	// they no longer use anything.
	used := make(map[string]api.Resources)
	for _, p := range pods {
		if p.NodeName != "" && p.Phase != api.PodSucceeded && p.Phase != api.PodFailed {
			used[p.NodeName] = used[p.NodeName].Add(p.Requests())
		}
	}

	want := pod.Requests()
	var fits []api.Node
	problems := make(map[string]int) // how many nodes have each problem
	for _, node := range nodes {
		if !node.Ready {
			problems["not ready"]++
			continue
		}
		problem := tooLittle(node, used[node.Name], want)
		if problem != "" {
			problems[problem]++
			continue
		}
		fits = append(fits, node)
	}

	if len(fits) == 0 {
		var parts []string
		for _, problem := range slices.Sorted(maps.Keys(problems)) {
			parts = append(parts, fmt.Sprintf("%d %s", problems[problem], problem))
		}
		return nil, fmt.Errorf("0/%d nodes are available: %s", len(nodes), strings.Join(parts, ", "))
	}
	return fits, nil
}

// tooLittle says what a node lacks for a pod that requests want, when its
// other pods already request used; or "" if the pod fits. A node that
// doesn't say how much of something it has isn't limited in it.
func tooLittle(node api.Node, used, want api.Resources) string {
	capacity := api.ParseResources(node.Capacity)
	if _, ok := node.Capacity[api.ResourceCPU]; ok && used.CPU+want.CPU > capacity.CPU {
		return "too little cpu"
	}
	if _, ok := node.Capacity[api.ResourceMemory]; ok && used.Memory+want.Memory > capacity.Memory {
		return "too little memory"
	}
	return ""
}
