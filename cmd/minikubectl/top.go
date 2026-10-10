package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// top shows what pods or nodes use right now, as measured by the kubelets.
func (c *cli) top(args []string) error {
	if len(args) != 1 || !(isPod(args[0]) || isNode(args[0])) {
		return errors.New("usage: minikubectl top pods|nodes")
	}

	nodes, err := c.client.ListNodes()
	if err != nil {
		return err
	}
	usage := make(map[string]api.Resources) // by pod
	onNode := make(map[string]api.Resources)
	var failures []string
	for _, node := range nodes {
		if !node.Ready || node.Address == "" {
			continue
		}
		metrics, err := client.GetPodMetrics(node.Address)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", node.Name, err))
			continue
		}
		for _, m := range metrics {
			u := api.ParseResources(m.Usage)
			usage[api.Key(m.Namespace, m.Name)] = u
			onNode[node.Name] = onNode[node.Name].Add(u)
		}
	}
	if len(usage) == 0 && len(failures) > 0 {
		return fmt.Errorf("no measurements: %s", strings.Join(failures, "; "))
	}

	if isNode(args[0]) {
		w := c.newTable("NAME\tCPU\tCPU%\tMEMORY\tMEMORY%")
		for _, node := range nodes {
			u := onNode[node.Name]
			capacity := api.ParseResources(node.Capacity)
			c.row(w, "", node.Name, api.FormatCPU(u.CPU), percent(u.CPU, capacity.CPU),
				api.FormatMemory(roundMi(u.Memory)), percent(u.Memory, capacity.Memory))
		}
		return w.Flush()
	}

	pods, err := c.client.ListPods(c.listNamespace())
	if err != nil {
		return err
	}
	w := c.newTable("NAME\tCPU\tMEMORY")
	for _, pod := range pods {
		u, ok := usage[api.Key(pod.Namespace, pod.Name)]
		if !ok {
			continue // not running, or not measured yet
		}
		c.row(w, pod.Namespace, pod.Name, api.FormatCPU(u.CPU), api.FormatMemory(roundMi(u.Memory)))
	}
	return w.Flush()
}

// percent returns part as a percentage of whole, or "-" if whole is unknown.
func percent(part, whole int64) string {
	if whole == 0 {
		return "-"
	}
	return fmt.Sprintf("%d%%", part*100/whole)
}

// roundMi rounds bytes to whole mebibytes, for showing.
func roundMi(bytes int64) int64 {
	return (bytes + 1<<19) >> 20 << 20
}

// autoscale creates a HorizontalPodAutoscaler for a Deployment:
//
//	autoscale deployment <name> [-min 1] -max N [-cpu-percent 80]
func (c *cli) autoscale(args []string) error {
	const usage = "usage: minikubectl autoscale deployment <name> [-min 1] -max <n> [-cpu-percent 80]"
	if len(args) < 2 || !isDeployment(args[0]) {
		return errors.New(usage)
	}
	name := args[1]

	fs := flag.NewFlagSet("autoscale", flag.ContinueOnError)
	minReplicas := fs.Int("min", 1, "fewest replicas")
	maxReplicas := fs.Int("max", 0, "most replicas")
	cpu := fs.Int("cpu-percent", 80, "target average CPU use, in percent of the pods' CPU request")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if *maxReplicas == 0 {
		return errors.New("-max is required\n" + usage)
	}

	h := api.HorizontalPodAutoscaler{
		TypeMeta:   api.TypeMetaFor("HorizontalPodAutoscaler"),
		ObjectMeta: api.ObjectMeta{Name: name, Namespace: c.namespace},
		HorizontalPodAutoscalerSpec: api.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: api.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: name},
			MinReplicas:    minReplicas,
			MaxReplicas:    *maxReplicas,
			Metrics: []api.MetricSpec{{Type: "Resource", Resource: &api.ResourceMetricSource{
				Name: "cpu", Target: api.MetricTarget{Type: "Utilization", AverageUtilization: cpu},
			}}},
		},
	}
	err := c.client.Autoscalers().Create(c.namespace, h)
	if err != nil {
		return err
	}
	fmt.Printf("horizontalpodautoscaler/%s autoscaled: %d to %d replicas, at %d%% cpu\n", name, *minReplicas, *maxReplicas, *cpu)
	return nil
}
