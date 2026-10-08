package scheduler

import (
	"errors"
	"strings"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// sized returns a node with some CPU and memory.
func sized(name, cpu, memory string) api.Node {
	n := node(name, true)
	n.Capacity = api.ResourceList{"cpu": cpu, "memory": memory}
	return n
}

// requesting returns a pod requesting some CPU and memory, bound to a node.
func requesting(nodeName, cpu, memory string, phase api.PodPhase) api.Pod {
	return api.Pod{
		PodSpec: api.PodSpec{NodeName: nodeName, Containers: []api.Container{{
			Resources: api.ResourceRequirements{Requests: api.ResourceList{"cpu": cpu, "memory": memory}},
		}}},
		PodStatus: api.PodStatus{Phase: phase},
	}
}

func TestFit(t *testing.T) {
	nodes := []api.Node{sized("small", "1", "512Mi"), sized("big", "4", "8Gi"), node("down", false)}
	pods := []api.Pod{
		requesting("small", "500m", "256Mi", api.PodRunning),
		requesting("small", "500m", "256Mi", api.PodSucceeded), // finished: doesn't count
	}

	fits, err := fit(requesting("", "500m", "256Mi", api.PodPending), nodes, pods)
	if err != nil || len(fits) != 2 {
		t.Errorf("a pod that fits both: got %d nodes, %v; want small and big", len(fits), err)
	}

	fits, err = fit(requesting("", "500m", "1Gi", api.PodPending), nodes, pods)
	if err != nil || len(fits) != 1 || fits[0].Name != "big" {
		t.Errorf("a pod needing 1Gi: got %v, %v; want only big", fits, err)
	}

	_, err = fit(requesting("", "8", "1Gi", api.PodPending), nodes, pods)
	want := "0/3 nodes are available: 1 not ready, 2 too little cpu"
	if err == nil || err.Error() != want {
		t.Errorf("a pod too big for every node: got %v, want %q", err, want)
	}
}

func TestFitWithoutCapacity(t *testing.T) {
	// A node created by hand, with no capacity, isn't limited.
	fits, err := fit(requesting("", "64", "1Ti", api.PodPending), []api.Node{node("n", true)}, nil)
	if err != nil || len(fits) != 1 {
		t.Errorf("got %v, %v; want the node", fits, err)
	}

	_, err = fit(api.Pod{}, nil, nil)
	if !errors.Is(err, ErrNoNodes) {
		t.Errorf("no nodes: got %v, want ErrNoNodes", err)
	}
}

func TestSchedulerReportsWhyNothingFits(t *testing.T) {
	_, err := fit(requesting("", "100m", "16Gi", api.PodPending), []api.Node{sized("n", "4", "8Gi")}, nil)
	if err == nil || !strings.Contains(err.Error(), "too little memory") {
		t.Errorf("got %v, want too little memory", err)
	}
}
