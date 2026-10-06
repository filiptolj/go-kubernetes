package scheduler

import (
	"errors"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

var twoNodes = []api.Node{
	{Name: "node-1", Ready: true},
	{Name: "node-2", Ready: true},
}

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
			pods:  []api.Pod{{NodeName: "node-1"}, {NodeName: "node-1"}, {NodeName: "node-2"}},
			want:  "node-2",
		},
		{
			name:  "ignores pods that aren't scheduled",
			nodes: twoNodes,
			pods:  []api.Pod{{NodeName: ""}, {NodeName: ""}, {NodeName: "node-1"}},
			want:  "node-2",
		},
		{
			name:  "skips nodes that aren't ready",
			nodes: []api.Node{{Name: "node-1", Ready: false}, {Name: "node-2", Ready: true}},
			pods:  []api.Pod{{NodeName: "node-2"}, {NodeName: "node-2"}},
			want:  "node-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&LeastLoaded{}).Pick(api.Pod{Name: "new"}, tt.nodes, tt.pods)
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
	notReady := []api.Node{{Name: "node-1", Ready: false}}

	for name, picker := range pickers {
		t.Run(name, func(t *testing.T) {
			_, err := picker.Pick(api.Pod{}, notReady, nil)
			if !errors.Is(err, ErrNoNodes) {
				t.Errorf("got error %v, want ErrNoNodes", err)
			}
		})
	}
}
