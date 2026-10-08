package api

import (
	"strings"
	"time"
)

// DefaultNamespace is the namespace used when none is given. It always exists.
const DefaultNamespace = "default"

// Key returns how an object in a namespace is identified across the whole
// cluster: "namespace/name". Names only need to be unique within a namespace.
func Key(namespace, name string) string {
	return namespace + "/" + name
}

// TypeMeta says what kind of object something is, and which version of the
// API describes it, as in "apiVersion: apps/v1, kind: Deployment".
type TypeMeta struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
}

// ObjectMeta is what every object has, whatever its kind: its name, its
// labels, and what the API server records about it.
//
// Objects embed it with the JSON name "metadata": in JSON it is nested,
// {"metadata": {"name": ...}}, but in Go its fields are promoted, so
// pod.Name works as well as pod.ObjectMeta.Name.
type ObjectMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	Labels    Labels `json:"labels,omitempty"`

	// The fields below are set by the API server.

	// UID tells objects with the same name apart: an object that is deleted
	// and created again gets a new one.
	UID string `json:"uid,omitempty"`

	// ResourceVersion changes every time the object changes. An update that
	// carries an older ResourceVersion is refused with a conflict, so a
	// change based on stale data can't overwrite a newer one. This is called
	// optimistic concurrency.
	ResourceVersion string `json:"resourceVersion,omitempty"`

	CreationTimestamp time.Time `json:"creationTimestamp,omitzero"`

	// OwnerReferences lists the object that created this one, such as the
	// ReplicaSet that created a pod.
	OwnerReferences []OwnerReference `json:"ownerReferences,omitempty"`
}

// OwnerReference points at the object that created another one, in the same
// namespace.
type OwnerReference struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	UID  string `json:"uid,omitempty"`

	// Controller is true for the owner that manages the object. Every owner
	// in this project is one.
	Controller bool `json:"controller,omitempty"`
}

// Owner returns the object that controls this one. ok is false if there is none.
func (m ObjectMeta) Owner() (ref OwnerReference, ok bool) {
	for _, ref := range m.OwnerReferences {
		if ref.Controller {
			return ref, true
		}
	}
	return OwnerReference{}, false
}

// OwnerName returns the name of the object that controls this one, or "".
func (m ObjectMeta) OwnerName() string {
	ref, _ := m.Owner()
	return ref.Name
}

// ControlledBy reports whether the object is controlled by an object of the
// given kind, such as "Job".
func (m ObjectMeta) ControlledBy(kind string) bool {
	ref, ok := m.Owner()
	return ok && ref.Kind == kind
}

// OwnedBy reports whether the object is controlled by the object of the
// given kind and name.
func (m ObjectMeta) OwnedBy(kind, name string) bool {
	ref, ok := m.Owner()
	return ok && ref.Kind == kind && ref.Name == name
}

// SetOwner records the object that controls this one. It needs a pointer
// receiver, because it changes the ObjectMeta.
func (m *ObjectMeta) SetOwner(kind, name, uid string) {
	m.OwnerReferences = []OwnerReference{{Kind: kind, Name: name, UID: uid, Controller: true}}
}

// GetObjectMeta returns the object's metadata. Every kind embeds ObjectMeta,
// so every kind gets this method too, which lets generic code reach any
// object's name and namespace.
func (m *ObjectMeta) GetObjectMeta() *ObjectMeta {
	return m
}

// MetaOf returns the metadata of obj, which must be a pointer to an object
// of some kind, such as *Pod.
func MetaOf(obj any) *ObjectMeta {
	return obj.(interface{ GetObjectMeta() *ObjectMeta }).GetObjectMeta()
}

// Labels are key/value tags on an object, such as "app": "web". Services and
// controllers use them to find their pods.
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

// Pod is the smallest thing Kubernetes runs: one or more containers.
type Pod struct {
	TypeMeta
	ObjectMeta `json:"metadata"`
	PodSpec    `json:"spec"`
	PodStatus  `json:"status"`
}

// PodSpec is what a pod should run.
type PodSpec struct {
	// InitContainers run one after the other, each until it exits, before
	// Containers start. A pod uses them to get ready: wait for a database,
	// fill a volume, and so on.
	InitContainers []Container `json:"initContainers,omitempty"`

	Containers    []Container   `json:"containers"`
	Volumes       []Volume      `json:"volumes,omitempty"`
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`

	// NodeName is the node the pod runs on. The scheduler sets it.
	NodeName string `json:"nodeName,omitempty"`
}

// PodPhase is the lifecycle state of a pod.
type PodPhase string

const (
	PodPending   PodPhase = "Pending"
	PodRunning   PodPhase = "Running"
	PodSucceeded PodPhase = "Succeeded"
	PodFailed    PodPhase = "Failed"
)

// PodStatus is how a pod is doing. The kubelet reports it; it is also the
// body of a status update.
type PodStatus struct {
	Phase PodPhase `json:"phase,omitempty"`

	// Ready is true once the pod can do its job: for a pod with a port, once
	// something answers on that port. Only ready pods get Service traffic.
	Ready bool `json:"ready,omitempty"`

	// Address is host:port where the pod's first port can be reached from the
	// node, and HostPorts has the same for every port the pod's containers
	// listen on, by container port.
	Address   string         `json:"address,omitempty"`
	HostPorts map[int]string `json:"hostPorts,omitempty"`

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

// Container describes one program to run inside a pod.
type Container struct {
	Name    string   `json:"name"`
	Image   string   `json:"image"`
	Command []string `json:"command,omitempty"`

	// Ports are the TCP ports the container listens on. The kubelet makes
	// them reachable and reports where in the pod's status.
	Ports []ContainerPort `json:"ports,omitempty"`

	// Env sets environment variables in the container.
	Env []EnvVar `json:"env,omitempty"`

	// VolumeMounts says where the pod's volumes appear inside the container.
	VolumeMounts []VolumeMount `json:"volumeMounts,omitempty"`

	// LivenessProbe checks that the container is still healthy. When it fails
	// too often in a row, the kubelet kills the container, which then
	// restarts as the pod's restart policy says. It needs a port.
	LivenessProbe *Probe `json:"livenessProbe,omitempty"`

	// ReadinessProbe checks that the container is ready for traffic. Without
	// one, a container with a port is ready once something answers on it.
	ReadinessProbe *Probe `json:"readinessProbe,omitempty"`

	// Resources says how much CPU and memory the container needs, and may use.
	Resources ResourceRequirements `json:"resources,omitzero"`
}

// ContainerPort is a port a container listens on.
type ContainerPort struct {
	Name          string `json:"name,omitempty"`
	ContainerPort int    `json:"containerPort"`
}

// Port returns the container's first port, or 0 if it has none. Probes
// check it, and the pod's Address points at it.
func (c Container) Port() int {
	if len(c.Ports) == 0 {
		return 0
	}
	return c.Ports[0].ContainerPort
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
	Name      string                 `json:"name"`
	EmptyDir  *EmptyDirSource        `json:"emptyDir,omitempty"`
	HostPath  *HostPathSource        `json:"hostPath,omitempty"`
	ConfigMap *ConfigMapVolumeSource `json:"configMap,omitempty"`
	Secret    *SecretVolumeSource    `json:"secret,omitempty"`
}

// EmptyDirSource has no settings: "emptyDir: {}" is enough.
type EmptyDirSource struct{}

// HostPathSource is a folder on the node.
type HostPathSource struct {
	Path string `json:"path"`
}

// ConfigMapVolumeSource names the ConfigMap whose keys become files.
type ConfigMapVolumeSource struct {
	Name string `json:"name"`
}

// SecretVolumeSource names the Secret whose keys become files.
type SecretVolumeSource struct {
	SecretName string `json:"secretName"`
}

// VolumeMount puts one of the pod's volumes at MountPath in a container.
type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// Probe checks a container through its first port: with an HTTP request if
// HTTPGet is set (a status from 200 to 399 means healthy), or else by
// checking that something answers on the port.
type Probe struct {
	// What to check. With httpGet, a GET request to the container's port
	// must answer with a status from 200 to 399. With exec, a command run
	// inside the container must exit with 0. With neither, something must
	// answer on the container's port.
	HTTPGet *HTTPGetAction `json:"httpGet,omitempty"`
	Exec    *ExecAction    `json:"exec,omitempty"`

	InitialDelaySeconds int `json:"initialDelaySeconds,omitempty"` // wait this long after the container starts; default 0
	PeriodSeconds       int `json:"periodSeconds,omitempty"`       // check this often; default 10
	FailureThreshold    int `json:"failureThreshold,omitempty"`    // fail this many times in a row to count as failed; default 3
	TimeoutSeconds      int `json:"timeoutSeconds,omitempty"`      // give up on one exec check after this long; default 3
}

// ExecAction is the command an exec Probe runs inside the container.
type ExecAction struct {
	Command []string `json:"command"`
}

// HTTPGetAction is the request a Probe sends.
type HTTPGetAction struct {
	Path string `json:"path"`
}

// Node is a machine that can run pods.
type Node struct {
	TypeMeta
	ObjectMeta `json:"metadata"`
	NodeStatus `json:"status"`
}

// NodeStatus is how a node is doing. Its kubelet reports it.
type NodeStatus struct {
	// Capacity is what the node offers, such as {"cpu": "4", "memory": "8192Mi"}.
	Capacity ResourceList `json:"capacity,omitempty"`

	Ready bool `json:"ready"`

	// Address is where the node's kubelet serves container logs, such as
	// "http://localhost:10250". Empty for nodes without a kubelet.
	Address string `json:"address,omitempty"`

	// LastHeartbeat is when the node's kubelet last checked in. The API server
	// sets it; it is zero for nodes that have no kubelet.
	LastHeartbeat time.Time `json:"lastHeartbeat,omitzero"`
}

// ResourceList is an amount of each resource, such as {"cpu": "500m",
// "memory": "256Mi"}.
type ResourceList map[string]string

// Namespace groups objects, so that teams or apps can use the same names
// without clashing. Nodes belong to the whole cluster; every other kind of
// object lives in a namespace.
type Namespace struct {
	TypeMeta
	ObjectMeta `json:"metadata"`
}

// EventType says what happened to an object.
type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
)

// WatchEvent describes one change to an object. A watch sends one of these,
// as a line of JSON, every time an object it watches changes. For DELETED,
// Object is the object as it was last.
type WatchEvent[T any] struct {
	Type   EventType `json:"type"`
	Object T         `json:"object"`
}

// PodEvent is a change to a pod.
type PodEvent = WatchEvent[Pod]

// Binding is the request to assign a pod to a node.
type Binding struct {
	NodeName string `json:"nodeName"`
}

// Scale is the request to change how many replicas an object wants.
type Scale struct {
	Replicas int `json:"replicas"`
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

// apiVersions says which version of the API each kind belongs to, and so
// which URL it lives under: "v1" kinds under /api/v1, the others under
// /apis/<group>/<version>, as in Kubernetes.
var apiVersions = map[string]string{
	"Pod": "v1", "Node": "v1", "Namespace": "v1", "Service": "v1", "ConfigMap": "v1", "Secret": "v1",
	"ReplicaSet": "apps/v1", "Deployment": "apps/v1", "StatefulSet": "apps/v1", "DaemonSet": "apps/v1",
	"Job": "batch/v1", "CronJob": "batch/v1",
	"Ingress": "networking.k8s.io/v1",
}

// kindsByPlural maps the plural used in URLs to its kind.
var kindsByPlural = map[string]string{
	"pods": "Pod", "nodes": "Node", "namespaces": "Namespace", "services": "Service",
	"configmaps": "ConfigMap", "secrets": "Secret", "events": "Event",
	"replicasets": "ReplicaSet", "deployments": "Deployment", "statefulsets": "StatefulSet", "daemonsets": "DaemonSet",
	"jobs": "Job", "cronjobs": "CronJob", "ingresses": "Ingress",
}

// APIVersion returns the API version of a kind, such as "apps/v1" for
// "Deployment". Kinds it doesn't know get "v1".
func APIVersion(kind string) string {
	if v, ok := apiVersions[kind]; ok {
		return v
	}
	return "v1"
}

// TypeMetaFor returns the TypeMeta of a kind.
func TypeMetaFor(kind string) TypeMeta {
	return TypeMeta{APIVersion: APIVersion(kind), Kind: kind}
}

// Prefix returns the start of the URL path for a kind, given its plural:
// "/api/v1" for "pods", "/apis/apps/v1" for "deployments".
func Prefix(plural string) string {
	version := APIVersion(kindsByPlural[plural])
	if !strings.Contains(version, "/") {
		return "/api/" + version
	}
	return "/apis/" + version
}
