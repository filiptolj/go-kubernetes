package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// isJob, isCronJob and the others report whether a resource name on the
// command line means that kind.
func isJob(r string) bool         { return r == "jobs" || r == "job" }
func isCronJob(r string) bool     { return r == "cronjobs" || r == "cronjob" || r == "cj" }
func isDaemonSet(r string) bool   { return r == "daemonsets" || r == "daemonset" || r == "ds" }
func isStatefulSet(r string) bool { return r == "statefulsets" || r == "statefulset" || r == "sts" }
func isConfigMap(r string) bool   { return r == "configmaps" || r == "configmap" || r == "cm" }
func isSecret(r string) bool      { return r == "secrets" || r == "secret" }

// applyResource reads an object of a newer kind from a file's data, puts it
// in namespace, and creates it, or updates it if it already exists.
func applyResource[T any](res *client.Resource[T], label string, data []byte, namespace string, meta func(*T) *api.Meta) error {
	var obj T
	err := json.Unmarshal(data, &obj)
	if err != nil {
		return err
	}
	m := meta(&obj)
	m.Namespace = namespace

	err = res.Create(namespace, obj)
	if err == nil {
		fmt.Printf("%s/%s created in namespace %s\n", label, m.Name, namespace)
		return nil
	}
	if !errors.Is(err, client.ErrConflict) {
		return err
	}

	err = res.Update(namespace, m.Name, obj)
	if err != nil {
		return err
	}
	fmt.Printf("%s/%s configured in namespace %s\n", label, m.Name, namespace)
	return nil
}

// applyWorkload applies a file of one of the newer kinds. It returns false
// if kind isn't one of them.
func (c *cli) applyWorkload(kind string, data []byte, namespace string) (bool, error) {
	switch kind {
	case "Job":
		return true, applyResource(c.client.Jobs(), "job", data, namespace, func(j *api.Job) *api.Meta { return &j.Meta })
	case "CronJob":
		return true, applyResource(c.client.CronJobs(), "cronjob", data, namespace, func(cj *api.CronJob) *api.Meta { return &cj.Meta })
	case "DaemonSet":
		return true, applyResource(c.client.DaemonSets(), "daemonset", data, namespace, func(d *api.DaemonSet) *api.Meta { return &d.Meta })
	case "StatefulSet":
		return true, applyResource(c.client.StatefulSets(), "statefulset", data, namespace, func(s *api.StatefulSet) *api.Meta { return &s.Meta })
	case "ConfigMap":
		return true, applyResource(c.client.ConfigMaps(), "configmap", data, namespace, func(cm *api.ConfigMap) *api.Meta { return &cm.Meta })
	case "Secret":
		return true, applyResource(c.client.Secrets(), "secret", data, namespace, func(s *api.Secret) *api.Meta { return &s.Meta })
	}
	return false, nil
}

// deleteWorkload deletes an object of one of the newer kinds. It returns
// false if resource isn't one of them.
func (c *cli) deleteWorkload(resource, name string) (bool, error) {
	var err error
	switch {
	case isJob(resource):
		err, resource = c.client.Jobs().Delete(c.namespace, name), "job"
	case isCronJob(resource):
		err, resource = c.client.CronJobs().Delete(c.namespace, name), "cronjob"
	case isDaemonSet(resource):
		err, resource = c.client.DaemonSets().Delete(c.namespace, name), "daemonset"
	case isStatefulSet(resource):
		err, resource = c.client.StatefulSets().Delete(c.namespace, name), "statefulset"
	case isConfigMap(resource):
		err, resource = c.client.ConfigMaps().Delete(c.namespace, name), "configmap"
	case isSecret(resource):
		err, resource = c.client.Secrets().Delete(c.namespace, name), "secret"
	default:
		return false, nil
	}

	if err == nil {
		fmt.Printf("%s/%s deleted from namespace %s\n", resource, name, c.namespace)
	}
	return true, err
}

// getWorkload prints the table for one of the newer kinds. It returns false
// if resource isn't one of them.
func (c *cli) getWorkload(resource string) (bool, error) {
	switch {
	case isJob(resource):
		return true, c.getJobs()
	case isCronJob(resource):
		return true, c.getCronJobs()
	case isDaemonSet(resource):
		return true, c.getDaemonSets()
	case isStatefulSet(resource):
		return true, c.getStatefulSets()
	case isConfigMap(resource):
		return true, getData(c, c.client.ConfigMaps().List, func(cm api.ConfigMap) (api.Meta, int) { return cm.Meta, len(cm.Data) })
	case isSecret(resource):
		return true, getData(c, c.client.Secrets().List, func(s api.Secret) (api.Meta, int) { return s.Meta, len(s.Data) })
	}
	return false, nil
}

func (c *cli) getJobs() error {
	jobs, err := c.client.Jobs().List(c.listNamespace())
	if err != nil {
		return err
	}

	w := c.newTable("NAME\tCOMPLETIONS\tACTIVE\tFAILED\tSTATUS\tDURATION")
	for _, job := range jobs {
		status := orNone(job.Status.Condition)
		if job.Status.Condition == "" {
			status = "Running"
		}
		duration := "-"
		if !job.Status.CompletionTime.IsZero() {
			duration = job.Status.CompletionTime.Sub(job.Status.StartTime).Round(1e9).String()
		}
		c.row(w, job.Namespace, job.Name, fmt.Sprintf("%d/%d", job.Status.Succeeded, job.Completions),
			job.Status.Active, job.Status.Failed, status, duration)
	}
	return w.Flush()
}

func (c *cli) getCronJobs() error {
	cronJobs, err := c.client.CronJobs().List(c.listNamespace())
	if err != nil {
		return err
	}

	w := c.newTable("NAME\tSCHEDULE\tSUSPEND\tLAST SCHEDULE")
	for _, cj := range cronJobs {
		last := "<never>"
		if !cj.Status.LastScheduleTime.IsZero() {
			last = age(cj.Status.LastScheduleTime) + " ago"
		}
		c.row(w, cj.Namespace, cj.Name, cj.Schedule, cj.Suspend, last)
	}
	return w.Flush()
}

func (c *cli) getDaemonSets() error {
	sets, err := c.client.DaemonSets().List(c.listNamespace())
	if err != nil {
		return err
	}
	nodes, err := c.client.ListNodes()
	if err != nil {
		return err
	}
	pods, err := c.client.ListPods(c.listNamespace())
	if err != nil {
		return err
	}

	readyNodes := 0
	for _, node := range nodes {
		if node.Ready {
			readyNodes++
		}
	}

	w := c.newTable("NAME\tDESIRED\tCURRENT\tREADY")
	for _, ds := range sets {
		current, ready := countOwned(pods, ds.Namespace, "DaemonSet", ds.Name)
		c.row(w, ds.Namespace, ds.Name, readyNodes, current, ready)
	}
	return w.Flush()
}

func (c *cli) getStatefulSets() error {
	sets, err := c.client.StatefulSets().List(c.listNamespace())
	if err != nil {
		return err
	}
	pods, err := c.client.ListPods(c.listNamespace())
	if err != nil {
		return err
	}

	w := c.newTable("NAME\tREADY")
	for _, ss := range sets {
		_, ready := countOwned(pods, ss.Namespace, "StatefulSet", ss.Name)
		c.row(w, ss.Namespace, ss.Name, fmt.Sprintf("%d/%d", ready, ss.Replicas))
	}
	return w.Flush()
}

// getData prints the table for ConfigMaps or Secrets: their names and how
// many keys they hold. list and describe are passed in, so one function
// serves both kinds.
func getData[T any](c *cli, list func(string) ([]T, error), describe func(T) (api.Meta, int)) error {
	objects, err := list(c.listNamespace())
	if err != nil {
		return err
	}

	w := c.newTable("NAME\tDATA")
	for _, obj := range objects {
		m, keys := describe(obj)
		c.row(w, m.Namespace, m.Name, keys)
	}
	return w.Flush()
}

// countOwned counts the alive pods, and the ready ones, that an object owns.
func countOwned(pods []api.Pod, namespace, kind, name string) (alive, ready int) {
	for _, pod := range pods {
		if pod.Namespace != namespace || !pod.OwnedBy(kind, name) {
			continue
		}
		if pod.Phase == api.PodPending || pod.Phase == api.PodRunning {
			alive++
		}
		if pod.Phase == api.PodRunning && pod.Ready {
			ready++
		}
	}
	return alive, ready
}

// describeWorkload describes an object of one of the newer kinds. It
// returns false if resource isn't one of them.
func (c *cli) describeWorkload(resource, name string) (bool, error) {
	switch {
	case isJob(resource):
		return true, c.describeJob(name)
	case isCronJob(resource):
		return true, c.describeCronJob(name)
	case isDaemonSet(resource):
		return true, c.describeDaemonSet(name)
	case isStatefulSet(resource):
		return true, c.describeStatefulSet(name)
	case isConfigMap(resource):
		cm, err := c.client.ConfigMaps().Get(c.namespace, name)
		if err != nil {
			return true, err
		}
		field("Name", cm.Name)
		field("Namespace", cm.Namespace)
		fmt.Println("Data:")
		for _, key := range sortedKeys(cm.Data) {
			fmt.Printf("  %s:\n%s\n", key, indent(cm.Data[key], "    "))
		}
		return true, nil
	case isSecret(resource):
		s, err := c.client.Secrets().Get(c.namespace, name)
		if err != nil {
			return true, err
		}
		field("Name", s.Name)
		field("Namespace", s.Namespace)
		fmt.Println("Data (values hidden):")
		for _, key := range sortedKeys(s.Data) {
			fmt.Printf("  %s: %d bytes\n", key, len(s.Data[key]))
		}
		return true, nil
	}
	return false, nil
}

func (c *cli) describeJob(name string) error {
	job, err := c.client.Jobs().Get(c.namespace, name)
	if err != nil {
		return err
	}

	backoffLimit := 6
	if job.BackoffLimit != nil {
		backoffLimit = *job.BackoffLimit
	}
	field("Name", job.Name)
	field("Namespace", job.Namespace)
	if job.Owner != "" {
		field("Owner", "CronJob/"+job.Owner)
	}
	field("Completions", fmt.Sprintf("%d (%d at a time)", job.Completions, job.Parallelism))
	field("Backoff", fmt.Sprintf("give up after %d failures", backoffLimit))
	field("Status", fmt.Sprintf("%d active, %d succeeded, %d failed  %s",
		job.Status.Active, job.Status.Succeeded, job.Status.Failed, job.Status.Condition))
	field("Started", formatTime(job.Status.StartTime))
	field("Finished", formatTime(job.Status.CompletionTime))
	printTemplate(job.Template)
	if err := c.printOwnedPods("Job", job.Name); err != nil {
		return err
	}
	return c.printEvents("Job", job.Namespace, job.Name)
}

func (c *cli) describeCronJob(name string) error {
	cj, err := c.client.CronJobs().Get(c.namespace, name)
	if err != nil {
		return err
	}
	jobs, err := c.client.Jobs().List(c.namespace)
	if err != nil {
		return err
	}

	field("Name", cj.Name)
	field("Namespace", cj.Namespace)
	field("Schedule", cj.Schedule)
	field("Suspend", yesNo(cj.Suspend))
	field("Last run", formatTime(cj.Status.LastScheduleTime))
	printTemplate(cj.JobTemplate.Template)

	fmt.Println("Jobs:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	count := 0
	for _, job := range jobs {
		if job.Owner == cj.Name {
			fmt.Fprintf(w, "  %s\t%d/%d\t%s\n", job.Name, job.Status.Succeeded, job.Completions, orNone(job.Status.Condition))
			count++
		}
	}
	w.Flush()
	if count == 0 {
		fmt.Println("  <none>")
	}
	return c.printEvents("CronJob", cj.Namespace, cj.Name)
}

func (c *cli) describeDaemonSet(name string) error {
	ds, err := c.client.DaemonSets().Get(c.namespace, name)
	if err != nil {
		return err
	}

	field("Name", ds.Name)
	field("Namespace", ds.Namespace)
	printTemplate(ds.Template)
	if err := c.printOwnedPods("DaemonSet", ds.Name); err != nil {
		return err
	}
	return c.printEvents("DaemonSet", ds.Namespace, ds.Name)
}

func (c *cli) describeStatefulSet(name string) error {
	ss, err := c.client.StatefulSets().Get(c.namespace, name)
	if err != nil {
		return err
	}

	field("Name", ss.Name)
	field("Namespace", ss.Namespace)
	field("Replicas", fmt.Sprint(ss.Replicas))
	printTemplate(ss.Template)
	if err := c.printOwnedPods("StatefulSet", ss.Name); err != nil {
		return err
	}
	return c.printEvents("StatefulSet", ss.Namespace, ss.Name)
}

// printOwnedPods lists the pods an object in the current namespace owns.
func (c *cli) printOwnedPods(kind, owner string) error {
	pods, err := c.client.ListPods(c.namespace)
	if err != nil {
		return err
	}

	fmt.Println("Pods:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	count := 0
	for _, pod := range pods {
		if pod.OwnedBy(kind, owner) {
			fmt.Fprintf(w, "  %s\t%s\tnode: %s\trestarts: %d\n", pod.Name, podStatus(pod), orNone(pod.NodeName), pod.Restarts)
			count++
		}
	}
	w.Flush()
	if count == 0 {
		fmt.Println("  <none>")
	}
	return nil
}

// podStatus is what the STATUS column shows: a problem like
// "CrashLoopBackOff" if there is one, or else the phase.
func podStatus(pod api.Pod) string {
	if pod.Reason != "" {
		return pod.Reason
	}
	return string(pod.Phase)
}

// sortedKeys returns a map's keys in order.
func sortedKeys(m map[string]string) []string {
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// indent puts prefix in front of every line of text.
func indent(text, prefix string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}
