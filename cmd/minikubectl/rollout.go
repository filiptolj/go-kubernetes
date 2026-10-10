package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

const rolloutUsage = `usage: minikubectl rollout status|history|undo deployment <name>
  status    wait until the newest version runs everywhere [-timeout 5m]
  history   list the versions (revisions) kept
  undo      go back to the previous version [-to-revision N]`

// rollout follows and controls the rolling updates of a Deployment.
func (c *cli) rollout(args []string) error {
	if len(args) < 2 {
		return errors.New(rolloutUsage)
	}
	action, rest := args[0], args[1:]

	// "deployment/web" or "deployment web"
	resource, name, found := strings.Cut(rest[0], "/")
	rest = rest[1:]
	if !found {
		if len(rest) == 0 {
			return errors.New(rolloutUsage)
		}
		name, rest = rest[0], rest[1:]
	}
	if !isDeployment(resource) {
		return fmt.Errorf("rollout only works for deployments, not %q", resource)
	}

	fs := flag.NewFlagSet("rollout", flag.ContinueOnError)
	timeout := fs.Duration("timeout", 5*time.Minute, "how long status waits")
	toRevision := fs.Int("to-revision", 0, "the revision undo goes back to (default: the one before the current)")
	if err := fs.Parse(rest); err != nil {
		return err
	}

	switch action {
	case "status":
		return c.rolloutStatus(name, *timeout)
	case "history":
		return c.rolloutHistory(name)
	case "undo":
		return c.rolloutUndo(name, *toRevision)
	}
	return errors.New(rolloutUsage)
}

// deploymentState is a Deployment with its ReplicaSets, newest revision first.
type deploymentState struct {
	d    api.Deployment
	sets []api.ReplicaSet
}

func (c *cli) deploymentState(name string) (deploymentState, error) {
	deployments, err := c.client.ListDeployments(c.namespace)
	if err != nil {
		return deploymentState{}, err
	}
	i := slices.IndexFunc(deployments, func(d api.Deployment) bool { return d.Name == name })
	if i < 0 {
		return deploymentState{}, fmt.Errorf("deployment %q not found in namespace %q", name, c.namespace)
	}
	all, err := c.client.ListReplicaSets(c.namespace)
	if err != nil {
		return deploymentState{}, err
	}
	sets := ownedBy(deployments[i], all)
	slices.SortFunc(sets, func(a, b api.ReplicaSet) int { return revisionOf(b) - revisionOf(a) })
	return deploymentState{deployments[i], sets}, nil
}

// revisionOf returns a ReplicaSet's revision, or 0 if it has none.
func revisionOf(rs api.ReplicaSet) int {
	n, _ := strconv.Atoi(rs.Annotations[api.RevisionAnnotation])
	return n
}

// rolloutStatus waits until a Deployment's newest version runs everywhere:
// all its replicas are ready, and no pods of older versions are left.
func (c *cli) rolloutStatus(name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := ""
	for {
		state, err := c.deploymentState(name)
		if err != nil {
			return err
		}
		pods, err := c.client.ListPods(c.namespace)
		if err != nil {
			return err
		}

		newest := newestReplicaSet(state.d, state.sets)
		var updated, ready, oldPods int
		for _, rs := range state.sets {
			alive, readyPods := countOwned(pods, rs.Namespace, "ReplicaSet", rs.Name)
			if rs.Name == newest {
				updated, ready = rs.Replicas, readyPods
			} else {
				oldPods += alive
			}
		}

		want := state.d.Replicas
		var msg string
		switch {
		case newest == "" || updated < want:
			msg = fmt.Sprintf("Waiting for deployment %q rollout to finish: %d of %d new replicas have been updated...", name, updated, want)
		case oldPods > 0:
			msg = fmt.Sprintf("Waiting for deployment %q rollout to finish: %d old replicas are pending termination...", name, oldPods)
		case ready < want:
			msg = fmt.Sprintf("Waiting for deployment %q rollout to finish: %d of %d updated replicas are available...", name, ready, want)
		default:
			fmt.Printf("deployment %q successfully rolled out\n", name)
			return nil
		}
		if msg != last {
			fmt.Println(msg)
			last = msg
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("deployment %q didn't finish rolling out within %s", name, timeout)
		}
		time.Sleep(time.Second)
	}
}

// rolloutHistory lists the versions of a Deployment that are kept.
func (c *cli) rolloutHistory(name string) error {
	state, err := c.deploymentState(name)
	if err != nil {
		return err
	}
	newest := newestReplicaSet(state.d, state.sets)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "REVISION\tREPLICASET\tREPLICAS\tIMAGES")
	for _, rs := range slices.Backward(state.sets) { // oldest first, like kubectl
		var images []string
		for _, ctr := range rs.Template.Containers {
			images = append(images, ctr.Image)
		}
		rev := strconv.Itoa(revisionOf(rs))
		if rs.Name == newest {
			rev += " (current)"
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", rev, rs.Name, rs.Replicas, strings.Join(images, ","))
	}
	return w.Flush()
}

// rolloutUndo goes back to an earlier version of a Deployment, by giving
// the Deployment that version's pod template again. The Deployment
// controller then rolls it out like any other change.
func (c *cli) rolloutUndo(name string, toRevision int) error {
	state, err := c.deploymentState(name)
	if err != nil {
		return err
	}
	newest := newestReplicaSet(state.d, state.sets)
	current := 0
	for _, rs := range state.sets {
		if rs.Name == newest {
			current = revisionOf(rs)
		}
	}

	var target *api.ReplicaSet
	for i, rs := range state.sets { // newest revision first
		rev := revisionOf(rs)
		if (toRevision == 0 && rev < current) || (toRevision != 0 && rev == toRevision) {
			target = &state.sets[i]
			break
		}
	}
	switch {
	case target == nil && toRevision != 0:
		return fmt.Errorf("deployment %q has no revision %d; see `minikubectl rollout history deployment %s`", name, toRevision, name)
	case target == nil:
		return fmt.Errorf("deployment %q has no earlier revision to go back to", name)
	case target.Name == newest:
		fmt.Printf("deployment/%s is already at revision %d\n", name, toRevision)
		return nil
	}

	d := state.d
	d.Template = target.Template
	err = c.client.UpdateDeployment(d)
	if err != nil {
		return err
	}
	fmt.Printf("deployment/%s rolled back to revision %d\n", name, revisionOf(*target))
	return nil
}
