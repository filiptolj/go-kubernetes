package controller

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// AutoscalerController runs the HorizontalPodAutoscalers: every Every, it
// measures how much CPU the pods of each autoscaled Deployment use, as a
// share of what they request, and changes the number of replicas to bring
// that share near the target. It works like Kubernetes' autoscaler:
//
//	desired replicas = ceil(pods measured × utilization / target)
//
// Nothing changes while the utilization is within 10% of the target. It
// grows at most to double the replicas at a time (or 4), and it only
// shrinks to the highest number it wanted during the last DownscaleWindow,
// so a short quiet moment doesn't remove pods that are needed again soon.
type AutoscalerController struct {
	Client          *client.Client
	Informers       *Informers    // where to read from; nil: from Client
	Every           time.Duration // how often to measure and decide
	DownscaleWindow time.Duration // how long to remember earlier decisions before shrinking

	// Metrics returns what each pod uses, by "namespace/name". If nil, every
	// node's kubelet is asked. Tests set it.
	Metrics func() (map[string]api.Resources, error)

	// Now returns the current time. If nil, time.Now. Tests set it.
	Now func() time.Time

	history map[string][]recommendation // by the autoscaler's "namespace/name"
}

// recommendation is a number of replicas the autoscaler wanted at some time.
type recommendation struct {
	at       time.Time
	replicas int
}

// Run checks the autoscalers every ac.Every, until ctx is cancelled.
func (ac *AutoscalerController) Run(ctx context.Context) {
	for _, s := range []client.Source{ac.Informers.autoscalers(), ac.Informers.pods(), ac.Informers.deployments(), ac.Informers.replicaSets(), ac.Informers.nodes()} {
		if !s.WaitForSync(ctx) {
			return
		}
	}
	ticker := time.NewTicker(ac.Every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ac.reconcileAll()
		}
	}
}

func (ac *AutoscalerController) now() time.Time {
	if ac.Now != nil {
		return ac.Now()
	}
	return time.Now()
}

// reconcileAll measures once, and then decides for every autoscaler.
func (ac *AutoscalerController) reconcileAll() {
	autoscalers, err := list(ac.Informers.autoscalers(), ac.Client.Autoscalers().List)
	if err != nil || len(autoscalers) == 0 {
		return
	}
	pods, err := list(ac.Informers.pods(), ac.Client.ListPods)
	if err != nil {
		log.Printf("autoscaler: %v", err)
		return
	}
	metrics, err := ac.metrics()
	if err != nil {
		log.Printf("autoscaler: %v", err)
		return
	}
	for _, h := range autoscalers {
		ac.reconcile(h, pods, metrics)
	}
}

// metrics returns what each pod uses, by "namespace/name".
func (ac *AutoscalerController) metrics() (map[string]api.Resources, error) {
	if ac.Metrics != nil {
		return ac.Metrics()
	}
	nodes, err := list(ac.Informers.nodes(), allNodes(ac.Client))
	if err != nil {
		return nil, err
	}

	usage := make(map[string]api.Resources)
	for _, node := range nodes {
		if !node.Ready || node.Address == "" {
			continue
		}
		pods, err := client.GetPodMetrics(node.Address)
		if err != nil {
			log.Printf("autoscaler: node %s: %v", node.Name, err)
			continue
		}
		for _, p := range pods {
			usage[api.Key(p.Namespace, p.Name)] = api.ParseResources(p.Usage)
		}
	}
	return usage, nil
}

// target returns how many replicas the object an autoscaler scales has,
// and the labels of its pods.
func (ac *AutoscalerController) target(h api.HorizontalPodAutoscaler) (replicas int, labels api.Labels, err error) {
	ref := h.ScaleTargetRef
	switch ref.Kind {
	case "Deployment":
		deployments, err := list(ac.Informers.deployments(), ac.Client.ListDeployments)
		if err != nil {
			return 0, nil, err
		}
		for _, d := range deployments {
			if d.Namespace == h.Namespace && d.Name == ref.Name {
				return d.Replicas, d.Template.Labels, nil
			}
		}
	case "ReplicaSet":
		sets, err := list(ac.Informers.replicaSets(), ac.Client.ListReplicaSets)
		if err != nil {
			return 0, nil, err
		}
		for _, rs := range sets {
			if rs.Namespace == h.Namespace && rs.Name == ref.Name {
				return rs.Replicas, rs.Template.Labels, nil
			}
		}
	}
	return 0, nil, fmt.Errorf("%s %q not found", ref.Kind, ref.Name)
}

// reconcile decides for one autoscaler.
func (ac *AutoscalerController) reconcile(h api.HorizontalPodAutoscaler, pods []api.Pod, metrics map[string]api.Resources) {
	key := api.Key(h.Namespace, h.Name)
	current, labels, err := ac.target(h)
	if err != nil {
		ac.events().Warning("HorizontalPodAutoscaler", h.Namespace, h.Name, "FailedGetScale", "%v", err)
		return
	}
	if current == 0 {
		return // scaled to zero by hand: autoscaling is off, as in Kubernetes
	}

	// Add up what the target's ready pods use and request.
	var used, requested int64
	measured, unrequested := 0, 0
	for _, pod := range pods {
		if pod.Namespace != h.Namespace || !pod.Labels.Matches(labels) || !isRunningAndReady(pod) {
			continue
		}
		request := pod.Requests().CPU
		if request == 0 {
			unrequested++
			continue
		}
		usage, ok := metrics[api.Key(pod.Namespace, pod.Name)]
		if !ok {
			continue // not measured yet
		}
		used += usage.CPU
		requested += request
		measured++
	}

	status := h.Status
	status.CurrentReplicas = current
	if measured == 0 {
		if unrequested > 0 {
			ac.events().Warning("HorizontalPodAutoscaler", h.Namespace, h.Name, "FailedGetResourceMetric",
				"the pods have no cpu request, so their utilization can't be worked out")
		}
		status.DesiredReplicas, status.CurrentCPUUtilization = current, nil
		ac.saveStatus(h, status)
		return
	}

	utilization := int(used * 100 / requested)
	target := h.TargetCPU()
	desired := current
	ratio := float64(utilization) / float64(target)
	if math.Abs(ratio-1) > 0.1 {
		desired = int(math.Ceil(float64(measured) * ratio))
	}
	desired = min(desired, max(2*current, 4)) // grow at most this fast
	desired = min(max(desired, h.Min()), h.MaxReplicas)

	// Shrink only as far as the highest recommendation in the window.
	now := ac.now()
	if ac.history == nil {
		ac.history = make(map[string][]recommendation)
	}
	var recent []recommendation
	for _, r := range ac.history[key] {
		if now.Sub(r.at) < ac.DownscaleWindow {
			recent = append(recent, r)
		}
	}
	recent = append(recent, recommendation{now, desired})
	ac.history[key] = recent
	if desired < current {
		for _, r := range recent {
			desired = max(desired, r.replicas)
		}
		desired = min(desired, current)
	}

	if desired != current {
		err := ac.scale(h, desired)
		if err != nil {
			log.Printf("autoscaler %s: %v", key, err)
			return
		}
		log.Printf("autoscaler %s: %s %q from %d to %d replicas: cpu at %d%% of request, target %d%%",
			key, h.ScaleTargetRef.Kind, h.ScaleTargetRef.Name, current, desired, utilization, target)
		ac.events().Normal("HorizontalPodAutoscaler", h.Namespace, h.Name, "SuccessfulRescale",
			"new size: %d; reason: cpu utilization %d%% of request, target %d%%", desired, utilization, target)
		status.LastScaleTime = now.UTC().Truncate(time.Second)
	}
	status.DesiredReplicas, status.CurrentCPUUtilization = desired, &utilization
	ac.saveStatus(h, status)
}

// scale sets the replicas of the autoscaler's target.
func (ac *AutoscalerController) scale(h api.HorizontalPodAutoscaler, replicas int) error {
	if h.ScaleTargetRef.Kind == "ReplicaSet" {
		return ac.Client.ScaleReplicaSet(h.Namespace, h.ScaleTargetRef.Name, replicas)
	}
	return ac.Client.ScaleDeployment(h.Namespace, h.ScaleTargetRef.Name, replicas)
}

// saveStatus writes an autoscaler's status, if it changed.
func (ac *AutoscalerController) saveStatus(h api.HorizontalPodAutoscaler, status api.HorizontalPodAutoscalerStatus) {
	old := h.Status
	same := old.CurrentReplicas == status.CurrentReplicas && old.DesiredReplicas == status.DesiredReplicas &&
		old.LastScaleTime.Equal(status.LastScaleTime) &&
		(old.CurrentCPUUtilization == nil) == (status.CurrentCPUUtilization == nil) &&
		(old.CurrentCPUUtilization == nil || *old.CurrentCPUUtilization == *status.CurrentCPUUtilization)
	if same {
		return
	}
	h.Status = status
	err := ac.Client.Autoscalers().UpdateStatus(h.Namespace, h.Name, h)
	if err != nil {
		log.Printf("autoscaler: %v", err)
	}
}

// events returns the autoscaler's event recorder.
func (ac *AutoscalerController) events() *client.Recorder {
	return ac.Client.Recorder("horizontal-pod-autoscaler")
}
