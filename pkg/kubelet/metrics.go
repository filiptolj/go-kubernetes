package kubelet

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// measureLoop measures what the running pods use every MetricsEvery, until
// ctx is cancelled. Measuring takes a while (docker stats looks at every
// container for a moment), so it happens here, in the background, and
// /stats answers with the latest numbers.
func (k *Kubelet) measureLoop(ctx context.Context) {
	ticker := time.NewTicker(k.cfg.MetricsEvery)
	defer ticker.Stop()
	for {
		k.measure()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// measure measures what every running pod uses.
func (k *Kubelet) measure() {
	k.mu.Lock()
	var pods []api.Pod
	for _, rp := range k.pods {
		if !rp.initializing && rp.phase == "" {
			pods = append(pods, rp.pod)
		}
	}
	k.mu.Unlock()

	var metrics []api.PodMetrics
	usage, err := k.runtime.PodUsage(pods)
	if err == nil {
		now := time.Now().UTC()
		for _, pod := range pods {
			u := usage[api.Key(pod.Namespace, pod.Name)]
			metrics = append(metrics, api.PodMetrics{
				Namespace: pod.Namespace,
				Name:      pod.Name,
				Timestamp: now,
				Usage:     api.ResourceList{api.ResourceCPU: api.FormatCPU(u.CPU), api.ResourceMemory: api.FormatMemory(u.Memory)},
			})
		}
	}

	k.metricsMu.Lock()
	k.metrics, k.metricsErr = metrics, err
	k.metricsMu.Unlock()
}

// handleStats answers GET /stats with what each running pod used at the
// last measurement. The autoscaler and `minikubectl top` read it.
func (k *Kubelet) handleStats(w http.ResponseWriter, r *http.Request) {
	k.metricsMu.Lock()
	metrics, err := k.metrics, k.metricsErr
	k.metricsMu.Unlock()

	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if metrics == nil {
		metrics = []api.PodMetrics{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}
