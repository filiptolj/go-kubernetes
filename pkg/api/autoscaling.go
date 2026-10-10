package api

import "time"

// HorizontalPodAutoscaler changes the number of replicas of a Deployment
// or ReplicaSet to keep the CPU its pods use near a target: the share of
// their CPU request, on average, such as 50%. More load means more pods.
type HorizontalPodAutoscaler struct {
	TypeMeta
	ObjectMeta                  `json:"metadata"`
	HorizontalPodAutoscalerSpec `json:"spec"`
	Status                      HorizontalPodAutoscalerStatus `json:"status"`
}

type HorizontalPodAutoscalerSpec struct {
	ScaleTargetRef CrossVersionObjectReference `json:"scaleTargetRef"`
	MinReplicas    *int                        `json:"minReplicas,omitempty"` // default 1
	MaxReplicas    int                         `json:"maxReplicas"`
	Metrics        []MetricSpec                `json:"metrics"`
}

// CrossVersionObjectReference names the object to scale, such as the
// Deployment web.
type CrossVersionObjectReference struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

// MetricSpec is what to measure. Only {type: Resource, resource: {name:
// cpu, target: {type: Utilization, averageUtilization: N}}} is supported.
type MetricSpec struct {
	Type     string                `json:"type"`
	Resource *ResourceMetricSource `json:"resource,omitempty"`
}

type ResourceMetricSource struct {
	Name   string       `json:"name"`
	Target MetricTarget `json:"target"`
}

type MetricTarget struct {
	Type               string `json:"type"`
	AverageUtilization *int   `json:"averageUtilization,omitempty"`
}

// HorizontalPodAutoscalerStatus is what the autoscaler last saw and did.
type HorizontalPodAutoscalerStatus struct {
	CurrentReplicas int `json:"currentReplicas"`
	DesiredReplicas int `json:"desiredReplicas"`

	// CurrentCPUUtilization is the pods' average CPU use as a share of
	// their request, in percent; nil until it could be measured.
	CurrentCPUUtilization *int      `json:"currentCPUUtilizationPercentage,omitempty"`
	LastScaleTime         time.Time `json:"lastScaleTime,omitzero"`
}

// TargetCPU returns the target CPU utilization in percent, or 0 if the
// autoscaler has none.
func (spec HorizontalPodAutoscalerSpec) TargetCPU() int {
	for _, m := range spec.Metrics {
		if m.Type == "Resource" && m.Resource != nil && m.Resource.Name == ResourceCPU &&
			m.Resource.Target.Type == "Utilization" && m.Resource.Target.AverageUtilization != nil {
			return *m.Resource.Target.AverageUtilization
		}
	}
	return 0
}

// Min returns the fewest replicas the autoscaler keeps.
func (spec HorizontalPodAutoscalerSpec) Min() int {
	if spec.MinReplicas != nil {
		return *spec.MinReplicas
	}
	return 1
}

// PodMetrics is how much CPU and memory a pod uses right now, as measured
// by its kubelet, like Kubernetes' metrics.k8s.io PodMetrics.
type PodMetrics struct {
	Namespace string       `json:"namespace"`
	Name      string       `json:"name"`
	Timestamp time.Time    `json:"timestamp"`
	Usage     ResourceList `json:"usage"` // cpu and memory
}
