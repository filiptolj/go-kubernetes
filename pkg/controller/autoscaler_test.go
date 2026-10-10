package controller

import (
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

func TestAutoscaler(t *testing.T) {
	st, c := newTestCluster(t)

	template := tmpl(api.Labels{"app": "web"}, "nginx")
	template.Containers[0].Resources.Requests = api.ResourceList{"cpu": "200m"}
	st.CreateDeployment(api.Deployment{ObjectMeta: meta("web"), DeploymentSpec: api.DeploymentSpec{Replicas: 2, Template: template}})
	for _, name := range []string{"web-a", "web-b"} {
		st.CreatePod(api.Pod{
			ObjectMeta: api.ObjectMeta{Name: name, Namespace: ns, Labels: api.Labels{"app": "web"}},
			PodSpec:    template.PodSpec,
			PodStatus:  api.PodStatus{Phase: api.PodRunning, Ready: true},
		})
	}
	fifty, one := 50, 1
	st.Autoscalers.Create(api.HorizontalPodAutoscaler{
		ObjectMeta: meta("web"),
		HorizontalPodAutoscalerSpec: api.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: api.CrossVersionObjectReference{Kind: "Deployment", Name: "web"},
			MinReplicas:    &one,
			MaxReplicas:    10,
			Metrics: []api.MetricSpec{{Type: "Resource", Resource: &api.ResourceMetricSource{
				Name: "cpu", Target: api.MetricTarget{Type: "Utilization", AverageUtilization: &fifty},
			}}},
		},
	})

	usage := int64(200) // millicores per pod
	clock := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	ac := &AutoscalerController{
		Client:          c,
		DownscaleWindow: 5 * time.Minute,
		Now:             func() time.Time { return clock },
		Metrics: func() (map[string]api.Resources, error) {
			return map[string]api.Resources{
				api.Key(ns, "web-a"): {CPU: usage},
				api.Key(ns, "web-b"): {CPU: usage},
			}, nil
		},
	}
	replicas := func() int { return st.ListDeployments(ns)[0].Replicas }

	// 100% of the request, with a 50% target: twice as many pods.
	ac.reconcileAll()
	if r := replicas(); r != 4 {
		t.Fatalf("at 100%%: got %d replicas, want 4", r)
	}
	h, _ := st.Autoscalers.Get(ns, "web")
	if h.Status.CurrentCPUUtilization == nil || *h.Status.CurrentCPUUtilization != 100 || h.Status.DesiredReplicas != 4 {
		t.Errorf("status: got %+v, want 100%% and 4 desired", h.Status)
	}

	// Within 10% of the target: nothing changes.
	usage = 105
	ac.reconcileAll()
	if r := replicas(); r != 4 {
		t.Errorf("at 52%%: got %d replicas, want still 4", r)
	}

	// Quiet: it would shrink to 1, but not before the window has passed.
	usage = 10
	clock = clock.Add(time.Minute)
	ac.reconcileAll()
	if r := replicas(); r != 4 {
		t.Errorf("quiet, within the window: got %d replicas, want still 4", r)
	}
	clock = clock.Add(5 * time.Minute)
	ac.reconcileAll()
	if r := replicas(); r != 1 {
		t.Errorf("quiet for longer than the window: got %d replicas, want 1", r)
	}

	// Never more than maxReplicas.
	usage = 5000
	for range 5 {
		ac.reconcileAll()
	}
	if r := replicas(); r != 10 {
		t.Errorf("under heavy load: got %d replicas, want the maximum, 10", r)
	}
}
