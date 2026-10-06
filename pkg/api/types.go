package api

import "time"

// DefaultNamespace is the namespace used when none is given. It always exists.
const DefaultNamespace = "default"

// Namespace groups objects, so that teams or apps can use the same names
// without clashing. Pods, ReplicaSets, Deployments, Services and events live
// in a namespace; nodes belong to the whole cluster.
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
}

// Pod is the smallest thing Kubernetes runs: one or more containers.
type Pod struct {
	Name       string      `json:"name"`
	Namespace  string      `json:"namespace"`
	Labels     Labels      `json:"labels,omitempty"`
	Containers []Container `json:"containers"`
	NodeName   string      `json:"nodeName,omitempty"`
	Phase      PodPhase    `json:"phase"`

	// Ready is true once the pod can do its job: for a pod with a port, once
	// something answers on that port. Only ready pods get Service traffic.
	Ready bool `json:"ready,omitempty"`

	// Owner is the name of the ReplicaSet that created this pod, if any. It is
	// always in the same namespace.
	Owner string `json:"owner,omitempty"`

	// Address is host:port where the pod's port can be reached, once it runs.
	Address string `json:"address,omitempty"`

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

// PodStatus is the request to change a pod's phase, readiness, and
// optionally its address.
type PodStatus struct {
	Phase   PodPhase `json:"phase"`
	Ready   bool     `json:"ready,omitempty"`
	Address string   `json:"address,omitempty"`
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

// PodTemplate describes the pods a ReplicaSet or Deployment creates.
type PodTemplate struct {
	Labels     Labels      `json:"labels,omitempty"`
	Containers []Container `json:"containers"`
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
