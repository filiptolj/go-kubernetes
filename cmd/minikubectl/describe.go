package main

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// describe prints everything about one object, followed by its events.
func (c *cli) describe(resource, name string) error {
	switch {
	case isPod(resource):
		return c.describePod(name)
	case isNode(resource):
		return c.describeNode(name)
	case isReplicaSet(resource):
		return c.describeReplicaSet(name)
	case isDeployment(resource):
		return c.describeDeployment(name)
	case isService(resource):
		return c.describeService(name)
	case isNamespace(resource):
		return c.describeNamespace(name)
	default:
		return fmt.Errorf("can't describe %q", resource)
	}
}

func (c *cli) describePod(name string) error {
	pod, err := c.client.GetPod(c.namespace, name)
	if err != nil {
		return err
	}

	field("Name", pod.Name)
	field("Namespace", pod.Namespace)
	field("Labels", formatLabels(pod.Labels))
	if pod.Owner != "" {
		field("Owner", "ReplicaSet/"+pod.Owner)
	} else {
		field("Owner", "<none>")
	}
	field("Node", orNone(pod.NodeName))
	field("Status", string(pod.Phase))
	field("Ready", yesNo(pod.Ready))
	field("Address", orNone(pod.Address))
	field("Started", formatTime(pod.StartedAt))
	field("Finished", formatTime(pod.FinishedAt))

	fmt.Println("Containers:")
	for _, ctr := range pod.Containers {
		fmt.Printf("  %s:\n", ctr.Name)
		fmt.Printf("    %-10s %s\n", "Image:", orNone(ctr.Image))
		fmt.Printf("    %-10s %s\n", "Command:", orNone(strings.Join(ctr.Command, " ")))
		if ctr.Port != 0 {
			fmt.Printf("    %-10s %d\n", "Port:", ctr.Port)
		}
	}

	return c.printEvents("Pod", pod.Namespace, pod.Name)
}

func (c *cli) describeNode(name string) error {
	nodes, err := c.client.ListNodes()
	if err != nil {
		return err
	}
	i := slices.IndexFunc(nodes, func(n api.Node) bool { return n.Name == name })
	if i < 0 {
		return fmt.Errorf("node %q not found", name)
	}
	node := nodes[i]

	status := "NotReady"
	if node.Ready {
		status = "Ready"
	}
	field("Name", node.Name)
	field("Status", status)
	field("CPU", fmt.Sprint(node.CPU))
	field("Memory", fmt.Sprintf("%dMi", node.Memory))
	field("Kubelet", orNone(node.Address))
	field("Heartbeat", formatTime(node.LastHeartbeat))

	// A node runs pods from every namespace.
	pods, err := c.client.ListPods("")
	if err != nil {
		return err
	}
	fmt.Println("Pods:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	count := 0
	for _, pod := range pods {
		if pod.NodeName == node.Name {
			fmt.Fprintf(w, "  %s\t%s\tready: %s\n", api.Key(pod.Namespace, pod.Name), pod.Phase, yesNo(pod.Ready))
			count++
		}
	}
	w.Flush()
	if count == 0 {
		fmt.Println("  <none>")
	}

	return c.printEvents("Node", "", node.Name)
}

func (c *cli) describeReplicaSet(name string) error {
	sets, err := c.client.ListReplicaSets(c.namespace)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(sets, func(rs api.ReplicaSet) bool { return rs.Name == name })
	if i < 0 {
		return fmt.Errorf("replicaset %q not found in namespace %q", name, c.namespace)
	}
	rs := sets[i]

	pods, err := c.client.ListPods(c.namespace)
	if err != nil {
		return err
	}
	var owned []api.Pod
	for _, pod := range pods {
		if pod.Owner == rs.Name {
			owned = append(owned, pod)
		}
	}

	field("Name", rs.Name)
	field("Namespace", rs.Namespace)
	if rs.Owner != "" {
		field("Owner", "Deployment/"+rs.Owner)
	} else {
		field("Owner", "<none>")
	}
	field("Replicas", fmt.Sprintf("%d desired, %d current", rs.Replicas, len(owned)))
	printTemplate(rs.Template)

	fmt.Println("Pods:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for _, pod := range owned {
		fmt.Fprintf(w, "  %s\t%s\tnode: %s\n", pod.Name, pod.Phase, orNone(pod.NodeName))
	}
	w.Flush()
	if len(owned) == 0 {
		fmt.Println("  <none>")
	}

	return c.printEvents("ReplicaSet", rs.Namespace, rs.Name)
}

func (c *cli) describeDeployment(name string) error {
	deployments, err := c.client.ListDeployments(c.namespace)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(deployments, func(d api.Deployment) bool { return d.Name == name })
	if i < 0 {
		return fmt.Errorf("deployment %q not found in namespace %q", name, c.namespace)
	}
	d := deployments[i]

	sets, err := c.client.ListReplicaSets(c.namespace)
	if err != nil {
		return err
	}
	pods, err := c.client.ListPods(c.namespace)
	if err != nil {
		return err
	}
	ready := readyByOwner(pods)

	field("Name", d.Name)
	field("Namespace", d.Namespace)
	field("Replicas", fmt.Sprint(d.Replicas))
	printTemplate(d.Template)

	fmt.Println("ReplicaSets:")
	newest := newestReplicaSet(d, sets)
	owned := ownedBy(d, sets)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for _, rs := range owned {
		version := "old"
		if rs.Name == newest {
			version = "current"
		}
		fmt.Fprintf(w, "  %s\t%d/%d ready\t(%s)\n", rs.Name, ready[api.Key(rs.Namespace, rs.Name)], rs.Replicas, version)
	}
	w.Flush()
	if len(owned) == 0 {
		fmt.Println("  <none>")
	}

	return c.printEvents("Deployment", d.Namespace, d.Name)
}

func (c *cli) describeService(name string) error {
	services, err := c.client.ListServices(c.namespace)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(services, func(s api.Service) bool { return s.Name == name })
	if i < 0 {
		return fmt.Errorf("service %q not found in namespace %q", name, c.namespace)
	}
	svc := services[i]

	// Only pods in the Service's own namespace can get its traffic.
	pods, err := c.client.ListPods(svc.Namespace)
	if err != nil {
		return err
	}

	field("Name", svc.Name)
	field("Namespace", svc.Namespace)
	field("Port", fmt.Sprint(svc.Port))
	field("Selector", formatLabels(svc.Selector))

	fmt.Println("Endpoints:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	count := 0
	for _, pod := range pods {
		if !pod.Labels.Matches(svc.Selector) {
			continue
		}
		state := "ready"
		if !serves(pod, svc) {
			state = "not ready: gets no traffic"
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\n", pod.Name, orNone(pod.Address), state)
		count++
	}
	w.Flush()
	if count == 0 {
		fmt.Println("  <none>: no pods in this namespace match the selector")
	}

	return c.printEvents("Service", svc.Namespace, svc.Name)
}

// describeNamespace shows how many objects of each kind live in a namespace.
func (c *cli) describeNamespace(name string) error {
	namespaces, err := c.client.ListNamespaces()
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(namespaces, func(ns api.Namespace) bool { return ns.Name == name }) {
		return fmt.Errorf("namespace %q not found", name)
	}

	pods, err := c.client.ListPods(name)
	if err != nil {
		return err
	}
	sets, err := c.client.ListReplicaSets(name)
	if err != nil {
		return err
	}
	deployments, err := c.client.ListDeployments(name)
	if err != nil {
		return err
	}
	services, err := c.client.ListServices(name)
	if err != nil {
		return err
	}

	running := 0
	for _, pod := range pods {
		if pod.Phase == api.PodRunning {
			running++
		}
	}

	field("Name", name)
	fmt.Println("Contains:")
	fmt.Printf("  %-13s %d (%d running)\n", "Pods:", len(pods), running)
	fmt.Printf("  %-13s %d\n", "ReplicaSets:", len(sets))
	fmt.Printf("  %-13s %d\n", "Deployments:", len(deployments))
	fmt.Printf("  %-13s %d\n", "Services:", len(services))
	return nil
}

// printTemplate prints a pod template's labels and containers.
func printTemplate(t api.PodTemplate) {
	fmt.Println("Pod template:")
	fmt.Printf("  %-12s %s\n", "Labels:", formatLabels(t.Labels))
	for _, ctr := range t.Containers {
		line := ctr.Image
		if len(ctr.Command) > 0 {
			line += "  " + strings.Join(ctr.Command, " ")
		}
		if ctr.Port != 0 {
			line += fmt.Sprintf("  (port %d)", ctr.Port)
		}
		fmt.Printf("  %-12s %s\n", ctr.Name+":", line)
	}
}

// printEvents prints the events about one object as a table.
func (c *cli) printEvents(kind, namespace, name string) error {
	events, err := c.client.ListEvents(namespace, kind, name)
	if err != nil {
		return err
	}

	fmt.Println("Events:")
	if len(events) == 0 {
		fmt.Println("  <none>")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "  TYPE\tREASON\tAGE\tFROM\tMESSAGE")
	for _, e := range events {
		when := age(e.LastSeen)
		if e.Count > 1 {
			when += fmt.Sprintf(" (x%d over %s)", e.Count, age(e.FirstSeen))
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\n", e.Type, e.Reason, when, e.Source, e.Message)
	}
	return w.Flush()
}

// field prints one "Key:  value" line, with the values lined up.
func field(key, value string) {
	fmt.Printf("%-12s %s\n", key+":", value)
}

// formatTime returns a time with how long ago it was, or "<none>".
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "<none>"
	}
	return t.Format("2006-01-02 15:04:05") + " (" + age(t) + " ago)"
}
