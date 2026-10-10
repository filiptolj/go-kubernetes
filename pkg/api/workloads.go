package api

import "time"

// PodTemplateSpec describes the pods a controller creates: their labels,
// and what they run.
type PodTemplateSpec struct {
	ObjectMeta `json:"metadata"`
	PodSpec    `json:"spec"`
}

// ReplicaSet keeps a fixed number of copies of a pod running.
type ReplicaSet struct {
	TypeMeta
	ObjectMeta     `json:"metadata"`
	ReplicaSetSpec `json:"spec"`
}

// ReplicaSetSpec is how many pods a ReplicaSet wants, and what they run.
type ReplicaSetSpec struct {
	Replicas int             `json:"replicas"`
	Template PodTemplateSpec `json:"template"`
}

// Deployment keeps a number of pods running from a template, like a
// ReplicaSet, but when the template changes it replaces the pods gradually:
// a rolling update.
type Deployment struct {
	TypeMeta
	ObjectMeta     `json:"metadata"`
	DeploymentSpec `json:"spec"`
}

// DeploymentSpec is how many pods a Deployment wants, and what they run.
type DeploymentSpec struct {
	Replicas int             `json:"replicas"`
	Template PodTemplateSpec `json:"template"`
}

// StatefulSet runs numbered copies of a pod with stable names: web-0, web-1,
// web-2. They are created one at a time, in order, each only once the one
// before is ready, and removed in reverse order. A replaced pod gets the same
// name back. Databases and other apps that care which copy is which use it.
type StatefulSet struct {
	TypeMeta
	ObjectMeta      `json:"metadata"`
	StatefulSetSpec `json:"spec"`
}

// StatefulSetSpec is how many pods a StatefulSet wants, and what they run.
type StatefulSetSpec struct {
	Replicas int             `json:"replicas"`
	Template PodTemplateSpec `json:"template"`
}

// DaemonSet runs one copy of a pod on every ready node, such as a log
// collector or a monitoring agent.
type DaemonSet struct {
	TypeMeta
	ObjectMeta    `json:"metadata"`
	DaemonSetSpec `json:"spec"`
}

// DaemonSetSpec is what a DaemonSet's pods run.
type DaemonSetSpec struct {
	Template PodTemplateSpec `json:"template"`
}

// Job runs pods until a number of them have succeeded: for work that has an
// end, such as a database migration or a report.
type Job struct {
	TypeMeta
	ObjectMeta `json:"metadata"`
	JobSpec    `json:"spec"`
	Status     JobStatus `json:"status"`
}

// JobSpec says what a Job runs, and how often it may try.
type JobSpec struct {
	Completions int `json:"completions,omitempty"` // pods that must succeed; default 1
	Parallelism int `json:"parallelism,omitempty"` // pods running at once; default 1

	// BackoffLimit is how many pods may fail before the whole Job counts as
	// failed; default 6. It is a pointer so that "not set" (nil) can be told
	// apart from 0, which means "no retries at all".
	BackoffLimit *int `json:"backoffLimit,omitempty"`

	// Template is the pod to run. Its restart policy must be OnFailure or
	// Never; the default is Never.
	Template PodTemplateSpec `json:"template"`
}

// Conditions a Job finishes with.
const (
	JobComplete = "Complete"
	JobFailed   = "Failed"
)

// JobStatus is how a Job is doing. The Job controller keeps it up to date.
type JobStatus struct {
	Active    int `json:"active"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`

	// Condition is "" while the Job runs, then JobComplete or JobFailed.
	Condition string `json:"condition,omitempty"`

	StartTime      time.Time `json:"startTime,omitzero"`
	CompletionTime time.Time `json:"completionTime,omitzero"`
}

// CronJob creates a Job on a schedule, like cron on Linux.
type CronJob struct {
	TypeMeta
	ObjectMeta  `json:"metadata"`
	CronJobSpec `json:"spec"`
	Status      CronJobStatus `json:"status"`
}

// CronJobSpec says when a CronJob runs, and what.
type CronJobSpec struct {
	// Schedule says when, in cron format: "minute hour day-of-month month
	// day-of-week", such as "*/5 * * * *" for every five minutes.
	Schedule string `json:"schedule"`

	// Suspend pauses the CronJob: no new Jobs are created while it is true.
	Suspend bool `json:"suspend,omitempty"`

	JobTemplate JobTemplateSpec `json:"jobTemplate"`
}

// JobTemplateSpec describes the Jobs a CronJob creates.
type JobTemplateSpec struct {
	Spec JobSpec `json:"spec"`
}

// CronJobStatus is how a CronJob is doing.
type CronJobStatus struct {
	// LastScheduleTime is the time the latest Job was due.
	LastScheduleTime time.Time `json:"lastScheduleTime,omitzero"`
}

// Service gives a group of pods one stable address. Connections to one of
// its ports on the proxy are forwarded to the ready pods in the Service's
// namespace whose labels match Selector. Ports are shared by the whole
// cluster: two Services can't use the same one, even in different namespaces.
type Service struct {
	TypeMeta
	ObjectMeta  `json:"metadata"`
	ServiceSpec `json:"spec"`
}

// ServiceSpec is which pods a Service sends traffic to, and on which ports.
type ServiceSpec struct {
	Selector Labels        `json:"selector"`
	Ports    []ServicePort `json:"ports"`
}

// ServicePort is one port of a Service: connections to Port go to TargetPort
// on the pods. TargetPort defaults to Port.
type ServicePort struct {
	Name       string `json:"name,omitempty"`
	Port       int    `json:"port"`
	TargetPort int    `json:"targetPort,omitempty"`
}

// Target returns the pod port a ServicePort sends traffic to.
func (p ServicePort) Target() int {
	if p.TargetPort == 0 {
		return p.Port
	}
	return p.TargetPort
}

// Ingress routes HTTP requests to Services by host name and path, such as
// shop.example.com/api to the Service api. The proxy serves them on its
// ingress port.
type Ingress struct {
	TypeMeta
	ObjectMeta  `json:"metadata"`
	IngressSpec `json:"spec"`
}

// IngressSpec is an Ingress's list of rules. A request goes to the first
// rule whose host matches, and within it to the path that is the longest
// prefix of the request's path.
type IngressSpec struct {
	Rules []IngressRule `json:"rules"`
}

// IngressRule routes the requests for one host name, or for any host if
// Host is empty.
type IngressRule struct {
	Host string           `json:"host,omitempty"`
	HTTP IngressRuleValue `json:"http"`
}

// IngressRuleValue holds a rule's paths.
type IngressRuleValue struct {
	Paths []IngressPath `json:"paths"`
}

// IngressPath sends requests whose path starts with Path to a Service.
// PathType is "Prefix" (the default) or "Exact".
type IngressPath struct {
	Path     string         `json:"path"`
	PathType string         `json:"pathType,omitempty"`
	Backend  IngressBackend `json:"backend"`
}

// IngressBackend names the Service, and the Service's port, to send to.
type IngressBackend struct {
	Service IngressServiceBackend `json:"service"`
}

type IngressServiceBackend struct {
	Name string             `json:"name"`
	Port ServiceBackendPort `json:"port"`
}

type ServiceBackendPort struct {
	Number int `json:"number"`
}

// ConfigMap holds settings for pods: as environment variables, or as files
// in a volume.
type ConfigMap struct {
	TypeMeta
	ObjectMeta `json:"metadata"`
	Data       map[string]string `json:"data"`
}

// Secret is like a ConfigMap, for passwords and keys.
//
// Its values are bytes. In JSON, Go writes a []byte as base64 text, which
// is exactly how Kubernetes shows Secrets: "cGFzc3dvcmQ=". To write a
// Secret by hand, put plain text in StringData instead; the API server moves
// it into Data. Note that base64 is not encryption: anyone who can read the
// Secret can decode it.
type Secret struct {
	TypeMeta
	ObjectMeta `json:"metadata"`
	Data       map[string][]byte `json:"data,omitempty"`
	StringData map[string]string `json:"stringData,omitempty"`
}
