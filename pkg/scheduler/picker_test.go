package scheduler

import (
	"errors"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// node returns a node with the given name and readiness.
func node(name string, ready bool) api.Node {
	return api.Node{ObjectMeta: api.ObjectMeta{Name: name}, NodeStatus: api.NodeStatus{Ready: ready}}
}

// on returns a pod bound to a node.
func on(nodeName string) api.Pod {
	return api.Pod{PodSpec: api.PodSpec{NodeName: nodeName}}
}

var twoNodes = []api.Node{node("node-1", true), node("node-2", true)}

func TestLeastLoaded(t *testing.T) {
	tests := []struct {
		name  string
		nodes []api.Node
		pods  []api.Pod
		want  string
	}{
		{
			name:  "empty nodes: picks the first",
			nodes: twoNodes,
			want:  "node-1",
		},
		{
			name:  "picks the node with fewer pods",
			nodes: twoNodes,
			pods:  []api.Pod{on("node-1"), on("node-1"), on("node-2")},
			want:  "node-2",
		},
		{
			name:  "ignores pods that aren't scheduled",
			nodes: twoNodes,
			pods:  []api.Pod{on(""), on(""), on("node-1")},
			want:  "node-2",
		},
		{
			name:  "skips nodes that aren't ready",
			nodes: []api.Node{node("node-1", false), node("node-2", true)},
			pods:  []api.Pod{on("node-2"), on("node-2")},
			want:  "node-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&LeastLoaded{}).Pick(api.Pod{ObjectMeta: api.ObjectMeta{Name: "new"}}, tt.nodes, tt.pods)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRoundRobinTakesTurns(t *testing.T) {
	rr := &RoundRobin{}

	want := []string{"node-1", "node-2", "node-1", "node-2"}
	for i, wantNode := range want {
		got, err := rr.Pick(api.Pod{}, twoNodes, nil)
		if err != nil {
			t.Fatalf("pick %d: unexpected error: %v", i, err)
		}
		if got != wantNode {
			t.Errorf("pick %d: got %q, want %q", i, got, wantNode)
		}
	}
}

// TestNoReadyNodes runs the same check against every Picker, through the
// interface. A new strategy only has to be added to this list.
func TestNoReadyNodes(t *testing.T) {
	pickers := map[string]Picker{
		"least-loaded": &LeastLoaded{},
		"round-robin":  &RoundRobin{},
	}
	notReady := []api.Node{node("node-1", false)}

	for name, picker := range pickers {
		t.Run(name, func(t *testing.T) {
			_, err := picker.Pick(api.Pod{}, notReady, nil)
			if !errors.Is(err, ErrNoNodes) {
				t.Errorf("got error %v, want ErrNoNodes", err)
			}
		})
	}
}
