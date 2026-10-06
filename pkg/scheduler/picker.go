package scheduler

import (
	"errors"
	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// ErrNoNodes means there is no ready node to put a pod on.
var ErrNoNodes = errors.New("no ready nodes")

// Picker chooses which node a pod should run on.
type Picker interface {
	Pick(pod api.Pod, nodes []api.Node, pods []api.Pod) (string, error)
}

// RoundRobin hands out ready nodes in turn: node-1, node-2, node-1, ...
type RoundRobin struct {
	next int
}

// Pick return the next ready node in line.
func (rr *RoundRobin) Pick(pod api.Pod, nodes []api.Node, pods []api.Pod) (string, error) {
	ready := readyNodes(nodes)
	if len(ready) == 0 {
		return "", ErrNoNodes
	}

	node := ready[rr.next%len(ready)]
	rr.next++
	return node.Name, nil
}

// LeastLoaded picks the ready node that has the fewest pods on it.
type LeastLoaded struct{}

// Pick return the ready node with the fewest pods.
func (ll *LeastLoaded) Pick(pod api.Pod, nodes []api.Node, pods []api.Pod) (string, error) {
	ready := readyNodes(nodes)
	if len(ready) == 0 {
		return "", ErrNoNodes
	}

	count := make(map[string]int)
	for _, p := range pods {
		if p.NodeName != "" {
			count[p.NodeName]++
		}
	}

	best := ready[0]
	for _, node := range ready[1:] {
		if count[node.Name] < count[best.Name] {
			best = node
		}
	}
	return best.Name, nil
}

// readyNodes returns only the nodes that are ready to run pods.
func readyNodes(nodes []api.Node) []api.Node {
	var ready []api.Node
	for _, node := range nodes {
		if node.Ready {
			ready = append(ready, node)
		}
	}
	return ready
}
