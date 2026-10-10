package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// get prints a table of one kind of object.
func (c *cli) get(resource string, watch bool) error {
	switch {
	case isPod(resource):
		return c.getPods(watch)
	case isNode(resource):
		return c.getNodes()
	case isReplicaSet(resource):
		return c.getReplicaSets()
	case isDeployment(resource):
		return c.getDeployments()
	case isService(resource):
		return c.getServices()
	case isEvent(resource):
		return c.getEvents()
	case isNamespace(resource):
		return c.getNamespaces()
	}

	ok, err := c.getWorkload(resource)
	if !ok {
		return fmt.Errorf("unknown resource %q", resource)
	}
	return err
}

// newTable returns a tabwriter for a table, and writes its header. With -A,
// every table gets a NAMESPACE column first; see c.row.
func (c *cli) newTable(header string) *tabwriter.Writer {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	if c.all {
		header = "NAMESPACE\t" + header
	}
	fmt.Fprintln(w, header)
	return w
}

// row writes one row of a table started with newTable. columns are joined
// with tabs; with -A, the namespace comes first.
func (c *cli) row(w io.Writer, namespace string, columns ...any) {
	var cells []string
	if c.all {
		cells = append(cells, namespace)
	}
	for _, col := range columns {
		cells = append(cells, fmt.Sprint(col))
	}
	fmt.Fprintln(w, strings.Join(cells, "\t"))
}

// getPods prints the pods table. With watch, it then prints a line for every
// change until you press Ctrl+C.
func (c *cli) getPods(watch bool) error {
	pods, err := list[api.Pod](c, "pods")
	if err != nil {
		return err
	}

	w := c.newTable("NAME\tSTATUS\tREADY\tRESTARTS\tNODE\tADDRESS")
	for _, pod := range pods {
		c.row(w, pod.Namespace, pod.Name, podStatus(pod), yesNo(pod.Ready), pod.Restarts, orNone(pod.NodeName), orNone(pod.Address))
	}
	w.Flush()

	if !watch {
		return nil
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	events, err := client.WatchList[api.Pod](ctx, c.client, "pods", c.listNamespace(), c.selector)
	if err != nil {
		return err
	}
	for event := range events {
		pod := event.Object
		if c.name != "" && pod.Name != c.name {
			continue
		}
		name := pod.Name
		if c.all {
			name = api.Key(pod.Namespace, pod.Name)
		}
		fmt.Printf("%-8s %-30s %-16s %s\n", event.Type, name, podStatus(pod), orNone(pod.NodeName))
	}

	if ctx.Err() != nil {
		return nil // the user pressed Ctrl+C
	}
	return errors.New("lost connection to the API server")
}

// getNodes prints the nodes table. Nodes belong to the whole cluster, not to
// a namespace.
func (c *cli) getNodes() error {
	nodes, err := list[api.Node](c, "nodes")
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tSTATUS\tCPU\tMEMORY\tLAST HEARTBEAT")
	for _, node := range nodes {
		status := "NotReady"
		if node.Ready {
			status = "Ready"
		}

		heartbeat := "<never>"
		if !node.LastHeartbeat.IsZero() {
			heartbeat = time.Since(node.LastHeartbeat).Round(time.Second).String() + " ago"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", node.Name, status, orNone(node.Capacity["cpu"]), orNone(node.Capacity["memory"]), heartbeat)
	}
	return w.Flush()
}

// getNamespaces prints the namespaces table.
func (c *cli) getNamespaces() error {
	namespaces, err := list[api.Namespace](c, "namespaces")
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME")
	for _, ns := range namespaces {
		fmt.Fprintln(w, ns.Name)
	}
	return w.Flush()
}

// getReplicaSets prints the ReplicaSets table: how many pods each one wants,
// how many it has, and how many of those are running.
func (c *cli) getReplicaSets() error {
	sets, err := list[api.ReplicaSet](c, "replicasets")
	if err != nil {
		return err
	}
	pods, err := c.client.ListPods(c.listNamespace())
	if err != nil {
		return err
	}

	// Count pods by the "namespace/name" of the ReplicaSet that owns them.
	current := make(map[string]int)
	running := make(map[string]int)
	for _, pod := range pods {
		if !pod.ControlledBy("ReplicaSet") {
			continue
		}
		owner := api.Key(pod.Namespace, pod.OwnerName())
		switch pod.Phase {
		case api.PodPending:
			current[owner]++
		case api.PodRunning:
			current[owner]++
			running[owner]++
		}
	}

	w := c.newTable("NAME\tDESIRED\tCURRENT\tRUNNING\tOWNER")
	for _, rs := range sets {
		key := api.Key(rs.Namespace, rs.Name)
		c.row(w, rs.Namespace, rs.Name, rs.Replicas, current[key], running[key], orNone(rs.OwnerName()))
	}
	return w.Flush()
}

// getDeployments prints the Deployments table: how many pods each wants,
// how many ready ones run the current version of its template, and how many
// are ready in total.
func (c *cli) getDeployments() error {
	deployments, err := list[api.Deployment](c, "deployments")
	if err != nil {
		return err
	}
	sets, err := c.client.ListReplicaSets(c.listNamespace())
	if err != nil {
		return err
	}
	pods, err := c.client.ListPods(c.listNamespace())
	if err != nil {
		return err
	}

	ready := readyByOwner(pods)

	w := c.newTable("NAME\tDESIRED\tUP-TO-DATE\tREADY\tIMAGES")
	for _, d := range deployments {
		upToDate, total := 0, 0
		newest := newestReplicaSet(d, sets)
		for _, rs := range ownedBy(d, sets) {
			n := ready[api.Key(rs.Namespace, rs.Name)]
			total += n
			if rs.Name == newest {
				upToDate = n
			}
		}

		var images []string
		for _, ctr := range d.Template.Containers {
			images = append(images, ctr.Image)
		}
		c.row(w, d.Namespace, d.Name, d.Replicas, upToDate, total, strings.Join(images, ","))
	}
	return w.Flush()
}

// readyByOwner counts the ready pods of each ReplicaSet, by its "namespace/name".
func readyByOwner(pods []api.Pod) map[string]int {
	ready := make(map[string]int)
	for _, pod := range pods {
		if pod.ControlledBy("ReplicaSet") && pod.Phase == api.PodRunning && pod.Ready {
			ready[api.Key(pod.Namespace, pod.OwnerName())]++
		}
	}
	return ready
}

// ownedBy returns the ReplicaSets that belong to a Deployment.
func ownedBy(d api.Deployment, sets []api.ReplicaSet) []api.ReplicaSet {
	var owned []api.ReplicaSet
	for _, rs := range sets {
		if rs.Namespace == d.Namespace && rs.OwnedBy("Deployment", d.Name) {
			owned = append(owned, rs)
		}
	}
	return owned
}

// newestReplicaSet returns the name of the Deployment's ReplicaSet whose
// template matches the Deployment's current one, or "" if there is none yet.
func newestReplicaSet(d api.Deployment, sets []api.ReplicaSet) string {
	for _, rs := range ownedBy(d, sets) {
		if sameTemplate(rs.Template, d.Template) {
			return rs.Name
		}
	}
	return ""
}

// sameTemplate reports whether two pod templates run the same containers.
func sameTemplate(a, b api.PodTemplateSpec) bool {
	return slices.EqualFunc(a.Containers, b.Containers, func(x, y api.Container) bool {
		return x.Name == y.Name && x.Image == y.Image && slices.Equal(x.Ports, y.Ports) && slices.Equal(x.Command, y.Command)
	})
}

// getServices prints the Services table, with how many ready pods each one
// currently forwards to.
func (c *cli) getServices() error {
	services, err := list[api.Service](c, "services")
	if err != nil {
		return err
	}
	pods, err := c.client.ListPods(c.listNamespace())
	if err != nil {
		return err
	}

	w := c.newTable("NAME\tPORTS\tSELECTOR\tPODS")
	for _, svc := range services {
		endpoints := 0
		for _, pod := range pods {
			if serves(pod, svc) {
				endpoints++
			}
		}
		c.row(w, svc.Namespace, svc.Name, servicePorts(svc), formatLabels(svc.Selector), endpoints)
	}
	return w.Flush()
}

// serves reports whether a pod gets a Service's traffic: it is in the
// Service's namespace, matches its selector, and is running and ready.
func serves(pod api.Pod, svc api.Service) bool {
	return pod.Namespace == svc.Namespace && pod.Labels.Matches(svc.Selector) &&
		pod.Phase == api.PodRunning && pod.Ready && pod.Address != ""
}

// getEvents prints the events in the namespace, or in every namespace with
// -A, oldest first. Events about nodes belong to no namespace: -A shows them.
func (c *cli) getEvents() error {
	events, err := c.client.ListEvents(c.listNamespace(), "", "")
	if err != nil {
		return err
	}

	w := c.newTable("AGE\tTYPE\tREASON\tOBJECT\tMESSAGE")
	for _, e := range events {
		when := age(e.LastSeen)
		if e.Count > 1 {
			when += fmt.Sprintf(" (x%d)", e.Count)
		}
		c.row(w, orNone(e.Namespace), when, e.Type, e.Reason, strings.ToLower(e.Kind)+"/"+e.Name, e.Message)
	}
	return w.Flush()
}

// orNone returns s, or "<none>" if it is empty.
func orNone(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

// yesNo returns "yes" or "no".
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// formatLabels returns labels as "a=1,b=2", sorted by key.
func formatLabels(labels api.Labels) string {
	if len(labels) == 0 {
		return "<none>"
	}
	var parts []string
	for key, value := range labels {
		parts = append(parts, key+"="+value)
	}
	slices.Sort(parts)
	return strings.Join(parts, ",")
}

// age returns how long ago t was, short like kubectl: "45s", "12m", "3h", "2d".
func age(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
