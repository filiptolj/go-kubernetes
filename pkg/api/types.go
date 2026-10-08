package api

import "time"

// DefaultNamespace is the namespace used when none is given. It always exists.
const DefaultNamespace = "default"

// Namespace groups objects, so that teams or apps can use the same names
// without clashing. Nodes belong to the whole cluster; every other kind of
// object lives in a namespace.
type Namespace struct {
	Name string `json:"name"`
}

// Key returns how an object in a namespace is identified across the whole
// cluster: "namespace/name". Names only need to be unique within a namespace.
func Key(namespace, name string) string {
	return namespace + "/" + name
}

// PodPhase is the lifecycle state of a pod.
type PodPhase string

const (
	PodPending   PodPhase = "Pending"
	PodRunning   PodPhase = "Running"
	PodSucceeded PodPhase = "Succeeded"
	PodFailed    PodPhase = "Failed"
)

// Labels are key/value tags on a pod, such as "app": "web". Services and
// Deployments use them to find their pods.
type Labels map[string]string

// Matches reports whether every key/value in selector is also in l.
// An empty selector matches nothing, so a typo never selects every pod.
func (l Labels) Matches(selector Labels) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if l[key] != value {
			return false
		}
	}
	return true
}

// Container describes one program to run inside a pod.
type Container struct {
	Name    string   `json:"name"`
	Image   string   `json:"image"`
	Command []string `json:"command,omitempty"`

	// Port is the TCP port the container listens on, if any. The kubelet
	// makes it reachable and reports where in the pod's Address.
	Port int `json:"port,omitempty"`

	// Env sets environment variables in the container.
	Env []EnvVar `json:"env,omitempty"`

	// VolumeMounts says where the pod's volumes appear inside the container.
	VolumeMounts []VolumeMount `json:"volumeMounts,omitempty"`

	// LivenessProbe checks that the container is still healthy. When it fails
	// too often in a row, the kubelet kills the container, which then
	// restarts as the pod's restart policy says. It needs a Port.
	LivenessProbe *Probe `json:"livenessProbe,omitempty"`

	// ReadinessProbe checks that the container is ready for traffic. Without
	// one, a container with a Port is ready once something answers on it.
	ReadinessProbe *Probe `json:"readinessProbe,omitempty"`
}

// RestartPolicy says what the kubelet does when one of a pod's containers exits.
type RestartPolicy string

const (
	// RestartAlways restarts the container, however it exited. It is the
	// default, for pods that should run forever, such as web servers.
	RestartAlways RestartPolicy = "Always"

	// RestartOnFailure restarts the container only if it failed (a non-zero
	// exit code). Good for work that should be retried until it succeeds.
	RestartOnFailure RestartPolicy = "OnFailure"

	// RestartNever leaves the container stopped. The pod finishes, Succeeded
	// or Failed, once all its containers have.
	RestartNever RestartPolicy = "Never"
)

// EnvVar is an environment variable: either a fixed Value, or a value read
// from a ConfigMap or a Secret.
type EnvVar struct {
	Name      string     `json:"name"`
	Value     string     `json:"value,omitempty"`
	ValueFrom *EnvSource `json:"valueFrom,omitempty"`
}

// EnvSource says where an environment variable's value comes from. Exactly
// one of the two must be set.
type EnvSource struct {
	ConfigMapKeyRef *KeyRef `json:"configMapKeyRef,omitempty"`
	SecretKeyRef    *KeyRef `json:"secretKeyRef,omitempty"`
}

// KeyRef points at one key of a ConfigMap or Secret in the pod's namespace.
type KeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// Volume is a folder a pod's containers can use. Exactly one of the sources
// must be set:
//   - EmptyDir: an empty folder, created for the pod and deleted with it.
//     Containers of the same pod can use it to share files.
//   - HostPath: a folder of the node itself.
//   - ConfigMap and Secret: a folder with one file per key.
type Volume struct {
	Name      string          `json:"name"`
	EmptyDir  *EmptyDirSource `json:"emptyDir,omitempty"`
	HostPath  *HostPathSource `json:"hostPath,omitempty"`
	ConfigMap *ObjectRef      `json:"configMap,omitempty"`
	Secret    *ObjectRef      `json:"secret,omitempty"`
}

// EmptyDirSource has no settings: "emptyDir": {} is enough.
type EmptyDirSource struct{}

// HostPathSource is a folder on the node.
type HostPathSource struct {
	Path string `json:"path"`
}

// ObjectRef points at an object by name, in the pod's namespace.
type ObjectRef struct {
	Name string `json:"name"`
}

// VolumeMount puts one of the pod's volumes at MountPath in a container.
type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// Probe checks a container through its Port: with an HTTP request if
// HTTPGet is set (a status from 200 to 399 means healthy), or else by
// checking that something answers on the port.
type Probe struct {
	HTTPGet *HTTPGetAction `json:"httpGet,omitempty"`

	InitialDelaySeconds int `json:"initialDelaySeconds,omitempty"` // wait this long after the container starts; default 0
	PeriodSeconds       int `json:"periodSeconds,omitempty"`       // check this often; default 10
	FailureThreshold    int `json:"failureThreshold,omitempty"`    // fail this many times in a row to count as failed; default 3
}

// HTTPGetAction is the request a Probe sends.
type HTTPGetAction struct {
	Path string `json:"path"`
}

// Pod is the smallest thing Kubernetes runs: one or more containers.
type Pod struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`

	// UID tells pods with the same name apart: a pod that is deleted and
	// created again (as StatefulSets do) gets a new one. The API server sets it.
	UID string `json:"uid,omitempty"`

	Labels        Labels        `json:"labels,omitempty"`
	Containers    []Container   `json:"containers"`
	Volumes       []Volume      `json:"volumes,omitempty"`
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`
	NodeName      string        `json:"nodeName,omitempty"`
	Phase         PodPhase      `json:"phase"`

	// Ready is true once the pod can do its job: for a pod with a port, once
	// something answers on that port. Only ready pods get Service traffic.
	Ready bool `json:"ready,omitempty"`

	// Owner is the name of the object that created this pod, if any, and
	// OwnerKind says what kind it is: "ReplicaSet", "Job", "DaemonSet" or
	// "StatefulSet". The owner is always in the same namespace. Pods saved
	// before OwnerKind existed have an empty one, which means "ReplicaSet".
	Owner     string `json:"owner,omitempty"`
	OwnerKind string `json:"ownerKind,omitempty"`

	// Address is host:port where the pod's port can be reached, once it runs.
	Address string `json:"address,omitempty"`

	// Restarts counts how often the kubelet has restarted the pod's containers.
	Restarts int `json:"restarts,omitempty"`

	// Reason explains a problem with a running pod, such as
	// "CrashLoopBackOff" while the kubelet waits to restart a container.
	Reason string `json:"reason,omitempty"`

	// StartedAt and FinishedAt are set by the API server when the pod's
	// phase becomes Running, and Succeeded or Failed.
	StartedAt  time.Time `json:"startedAt,omitzero"`
	FinishedAt time.Time `json:"finishedAt,omitzero"`
}

// Node is a machine that can run pods.
type Node struct {
	Name   string `json:"name"`
	CPU    int    `json:"cpu"`
	Memory int    `json:"memory"`
	Ready  bool   `json:"ready"`

	// Address is where the node's kubelet serves container logs, such as
	// "http://localhost:10250". Empty for nodes without a kubelet.
	Address string `json:"address,omitempty"`

	// LastHeartbeat is when the node's kubelet last checked in. The API server
	// sets it; it is zero for nodes that have no kubelet.
	LastHeartbeat time.Time `json:"lastHeartbeat,omitzero"`
}

// EventType says what happened to a pod.
type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
)

// PodEvent describes one change to a pod. Watchers receive these.
type PodEvent struct {
	Type EventType `json:"type"`
	Pod  Pod       `json:"pod"`
}

// Binding is the request to assign a pod to a node.
type Binding struct {
	NodeName string `json:"nodeName"`
}

// PodStatus is the request to change a pod's phase, readiness and other
// status. The kubelet sends it.
type PodStatus struct {
	Phase    PodPhase `json:"phase"`
	Ready    bool     `json:"ready,omitempty"`
	Address  string   `json:"address,omitempty"`
	Restarts int      `json:"restarts,omitempty"`
	Reason   string   `json:"reason,omitempty"`
}

// ControlledBy reports whether a pod was created by an object of the given
// kind, such as "Job".
func (p Pod) ControlledBy(kind string) bool {
	if p.Owner == "" {
		return false
	}
	ownerKind := p.OwnerKind
	if ownerKind == "" {
		ownerKind = "ReplicaSet" // saved before OwnerKind existed
	}
	return ownerKind == kind
}

// OwnedBy reports whether a pod was created by the object of the given kind
// and name, in the pod's namespace.
func (p Pod) OwnedBy(kind, name string) bool {
	return p.ControlledBy(kind) && p.Owner == name
}

// NodeStatus is the request to mark a node Ready or NotReady.
type NodeStatus struct {
	Ready bool `json:"ready"`
}

// ReplicaSet keeps a fixed number of copies of a pod running.
type ReplicaSet struct {
	Name      string      `json:"name"`
	Namespace string      `json:"namespace"`
	Replicas  int         `json:"replicas"`
	Template  PodTemplate `json:"template"`

	// Owner is the name of the Deployment that created this ReplicaSet, if
	// any. It is always in the same namespace.
	Owner string `json:"owner,omitempty"`
}

// PodTemplate describes the pods a controller creates.
type PodTemplate struct {
	Labels        Labels        `json:"labels,omitempty"`
	Containers    []Container   `json:"containers"`
	Volumes       []Volume      `json:"volumes,omitempty"`
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`
}

// Scale is the request to change how many replicas a ReplicaSet or
// Deployment wants.
type Scale struct {
	Replicas int `json:"replicas"`
}

// Deployment keeps a number of pods running from a template, like a
// ReplicaSet, but when the template changes it replaces the pods gradually:
// a rolling update.
type Deployment struct {
	Name      string      `json:"name"`
	Namespace string      `json:"namespace"`
	Replicas  int         `json:"replicas"`
	Template  PodTemplate `json:"template"`
}

// Service gives a group of pods one stable address. Connections to Port on
// the proxy are forwarded to the ready pods in the Service's namespace whose
// labels match Selector. Ports are shared by the whole cluster: two Services
// can't use the same one, even in different namespaces.
type Service struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Port      int    `json:"port"`
	Selector  Labels `json:"selector"`
}

// Event types.
const (
	EventNormal  = "Normal"
	EventWarning = "Warning"
)

// Event records something that happened to an object, such as "Scheduled"
// or "BackOff". `minikubectl describe` shows an object's events.
type Event struct {
	ID string `json:"id"` // set by the API server

	// The object the event is about, such as kind "Pod" and name "web-x7k2p".
	// Namespace is empty for objects that don't live in one, such as nodes.
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`

	Type    string `json:"type"`    // EventNormal or EventWarning
	Reason  string `json:"reason"`  // a short CamelCase word, such as "Scheduled"
	Message string `json:"message"` // what happened, for humans
	Source  string `json:"source"`  // the component that saw it, such as "scheduler"

	// When the same event happens again, Count goes up and LastSeen moves,
	// instead of a new event being added.
	Count     int       `json:"count"`
	FirstSeen time.Time `json:"firstSeen"`
	LastSeen  time.Time `json:"lastSeen"`
}
