package store

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// ErrNotFound means the object doesn't exist.
var ErrNotFound = errors.New("not found")

// ErrConflict means the request clashes with the current state.
var ErrConflict = errors.New("conflict")

// Store keeps the cluster's objects in memory, and also in a Backend (a file
// or etcd) if it was opened with Open, so they survive restarts. It is safe
// to use from many requests at once.
//
// Objects that live in a namespace are kept under the key "namespace/name"
// (see api.Key); nodes and namespaces under their name.
//
// Every change is written to the backend first and only then made in
// memory. If the backend fails, the change is refused, so memory and
// backend never disagree.
type Store struct {
	mu          sync.Mutex
	backend     Backend // nil: memory only
	pods        map[string]api.Pod
	nodes       map[string]api.Node
	replicaSets map[string]api.ReplicaSet
	deployments map[string]api.Deployment
	services    map[string]api.Service
	namespaces  map[string]api.Namespace

	// The newer kinds, stored with the generic Resource type. resources
	// lists them all, so the store can load and clean them in one loop.
	Jobs         *Resource[api.Job]
	CronJobs     *Resource[api.CronJob]
	DaemonSets   *Resource[api.DaemonSet]
	StatefulSets *Resource[api.StatefulSet]
	ConfigMaps   *Resource[api.ConfigMap]
	Secrets      *Resource[api.Secret]
	Ingresses    *Resource[api.Ingress]
	Autoscalers  *Resource[api.HorizontalPodAutoscaler]
	Leases       *Resource[api.Lease]
	resources    []resource

	events      []api.Event
	nextEventID int
	watchers    map[int]*watcher
	nextID      int

	// version is the last resourceVersion handed out. See meta.go.
	version uint64
}

// New returns a Store that keeps everything in memory only. It starts with
// just the default namespace.
func New() *Store {
	s := &Store{
		pods:        make(map[string]api.Pod),
		nodes:       make(map[string]api.Node),
		replicaSets: make(map[string]api.ReplicaSet),
		deployments: make(map[string]api.Deployment),
		services:    make(map[string]api.Service),
		namespaces: map[string]api.Namespace{
			api.DefaultNamespace: {
				TypeMeta:   api.TypeMetaFor("Namespace"),
				ObjectMeta: api.ObjectMeta{Name: api.DefaultNamespace},
			},
		},
		watchers: make(map[int]*watcher),
	}

	s.Jobs = newResource[api.Job](s, "jobs", "job")
	s.CronJobs = newResource[api.CronJob](s, "cronJobs", "cronjob")
	s.DaemonSets = newResource[api.DaemonSet](s, "daemonSets", "daemonset")
	s.StatefulSets = newResource[api.StatefulSet](s, "statefulSets", "statefulset")
	s.ConfigMaps = newResource[api.ConfigMap](s, "configMaps", "configmap")
	s.Secrets = newResource[api.Secret](s, "secrets", "secret")
	s.Ingresses = newResource[api.Ingress](s, "ingresses", "ingress")
	s.Autoscalers = newResource[api.HorizontalPodAutoscaler](s, "horizontalPodAutoscalers", "horizontalpodautoscaler")
	s.Leases = newResource[api.Lease](s, "leases", "lease")
	return s
}

// CreatePod saves a new pod. It fails if a pod with that name already exists.
func (s *Store) CreatePod(pod api.Pod) (api.Pod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.checkNamespace(pod.Namespace)
	if err != nil {
		return api.Pod{}, err
	}

	key := api.Key(pod.Namespace, pod.Name)
	_, exists := s.pods[key]
	if exists {
		return api.Pod{}, fmt.Errorf("pod %q already exists in namespace %q: %w", pod.Name, pod.Namespace, ErrConflict)
	}

	s.stampNew(&pod.ObjectMeta)

	err = s.put(api.EventAdded, kindPods, key, pod)
	if err != nil {
		return api.Pod{}, err
	}
	s.pods[key] = pod
	return pod, nil
}

// ListPods returns the pods in a namespace, or in all namespaces if
// namespace is "", sorted by namespace and name.
func (s *Store) ListPods(namespace string) []api.Pod {
	s.mu.Lock()
	defer s.mu.Unlock()

	return inNamespace(s.pods, namespace)
}

// GetPod returns a pod. The bool is false if it doesn't exist.
func (s *Store) GetPod(namespace, name string) (api.Pod, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pod, ok := s.pods[api.Key(namespace, name)]
	return pod, ok
}

// CreateNode saves a new node. It fails if a node with that name already exists.
func (s *Store) CreateNode(node api.Node) (api.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.nodes[node.Name]
	if exists {
		return api.Node{}, fmt.Errorf("node %q already exists: %w", node.Name, ErrConflict)
	}

	s.stampNew(&node.ObjectMeta)
	err := s.put(api.EventAdded, kindNodes, node.Name, node)
	if err != nil {
		return api.Node{}, err
	}
	s.nodes[node.Name] = node
	return node, nil
}

// ListNodes returns every node, sorted by name.
func (s *Store) ListNodes() []api.Node {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sortedValues(s.nodes)
}

// BindPod assigns a pod to a node. It fails if either one doesn't exist,
// or if the pod is already bound.
func (s *Store) BindPod(namespace, podName, nodeName string) (api.Pod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, podName)
	pod, ok := s.pods[key]
	if !ok {
		return api.Pod{}, fmt.Errorf("pod %q in namespace %q: %w", podName, namespace, ErrNotFound)
	}

	_, ok = s.nodes[nodeName]
	if !ok {
		return api.Pod{}, fmt.Errorf("node %q: %w", nodeName, ErrNotFound)
	}

	if pod.NodeName != "" {
		return api.Pod{}, fmt.Errorf("pod %q is already bound to node %q: %w", podName, pod.NodeName, ErrConflict)
	}

	pod.NodeName = nodeName
	pod.ResourceVersion = s.nextVersion()
	err := s.put(api.EventModified, kindPods, key, pod)
	if err != nil {
		return api.Pod{}, err
	}
	s.pods[key] = pod
	return pod, nil
}

// SetPodStatus changes a pod's phase, for example from Pending to Running,
// and its address if status has one. It also records when the pod started
// running and when it finished.
func (s *Store) SetPodStatus(namespace, name string, status api.PodStatus) (api.Pod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, name)
	pod, ok := s.pods[key]
	if !ok {
		return api.Pod{}, fmt.Errorf("pod %q in namespace %q: %w", name, namespace, ErrNotFound)
	}

	// A finished pod stays finished, the way it finished. This stops a late
	// report from bringing a pod back after it was reported Failed, or from
	// turning a success into a failure.
	finished := pod.Phase == api.PodSucceeded || pod.Phase == api.PodFailed
	if finished && status.Phase != pod.Phase {
		return api.Pod{}, fmt.Errorf("pod %q has already finished (%s): %w", name, pod.Phase, ErrConflict)
	}

	now := time.Now()
	switch status.Phase {
	case api.PodRunning:
		if pod.StartedAt.IsZero() {
			pod.StartedAt = now
		}
	case api.PodSucceeded, api.PodFailed:
		if pod.FinishedAt.IsZero() {
			pod.FinishedAt = now
		}
	}

	pod.Phase = status.Phase
	pod.Ready = status.Ready && status.Phase == api.PodRunning
	pod.Restarts = status.Restarts
	pod.Reason = status.Reason
	if status.Address != "" {
		pod.Address = status.Address
	}
	if status.HostPorts != nil {
		pod.HostPorts = status.HostPorts
	}
	if status.PodIP != "" {
		pod.PodIP = status.PodIP
	}
	pod.ResourceVersion = s.nextVersion()

	err := s.put(api.EventModified, kindPods, key, pod)
	if err != nil {
		return api.Pod{}, err
	}
	s.pods[key] = pod
	return pod, nil
}

// PutNode creates a node, or replaces it if it already exists, and records a
// heartbeat. Kubelets call it to register their node and then to keep it alive.
func (s *Store) PutNode(node api.Node) (api.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node.LastHeartbeat = time.Now()
	event := api.EventModified
	if old, ok := s.nodes[node.Name]; ok {
		err := s.stampUpdate(&node.ObjectMeta, old.ObjectMeta)
		if err != nil {
			return api.Node{}, err
		}
	} else {
		s.stampNew(&node.ObjectMeta)
		event = api.EventAdded
	}
	err := s.put(event, kindNodes, node.Name, node)
	if err != nil {
		return api.Node{}, err
	}
	s.nodes[node.Name] = node
	return node, nil
}

// DeletePod removes a pod and tells watchers, so its kubelet can stop it.
func (s *Store) DeletePod(namespace, name string) (api.Pod, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, name)
	pod, ok := s.pods[key]
	if !ok {
		return api.Pod{}, fmt.Errorf("pod %q in namespace %q: %w", name, namespace, ErrNotFound)
	}

	err := s.deletePod(key, pod)
	if err != nil {
		return api.Pod{}, err
	}
	return pod, nil
}

// deletePod removes a pod and tells watchers. The caller must hold s.mu.
// DeletePodGracefully deletes a pod the way a client asks to: a pod that
// may be running on a node is only marked for deletion, and its kubelet
// gives its containers grace seconds to stop, then deletes it for good with
// grace 0. A grace below 0 means the pod's own grace period. Asking again
// with a shorter grace shortens it. removed says whether the pod is gone.
func (s *Store) DeletePodGracefully(namespace, name string, grace int) (pod api.Pod, removed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, name)
	pod, ok := s.pods[key]
	if !ok {
		return api.Pod{}, false, fmt.Errorf("pod %q in namespace %q: %w", name, namespace, ErrNotFound)
	}
	if grace < 0 {
		grace = pod.GracePeriod()
	}

	// Nothing runs for a pod that has no node yet, or has finished: it can
	// go at once.
	finished := pod.Phase == api.PodSucceeded || pod.Phase == api.PodFailed
	if grace == 0 || pod.NodeName == "" || finished {
		return pod, true, s.deletePod(key, pod)
	}

	if pod.Terminating() && *pod.DeletionGracePeriodSeconds <= grace {
		return pod, false, nil // already terminating, at least as quickly
	}
	s.markForDeletion(&pod.ObjectMeta)
	pod.DeletionGracePeriodSeconds = &grace
	err = s.put(api.EventModified, kindPods, key, pod)
	if err != nil {
		return api.Pod{}, false, err
	}
	s.pods[key] = pod
	return pod, false, nil
}

func (s *Store) deletePod(key string, pod api.Pod) error {
	err := s.remove(kindPods, key, pod)
	if err != nil {
		return err
	}
	delete(s.pods, key)
	return nil
}

// SetNodeReady marks a node Ready or NotReady. Unlike PutNode, it leaves the
// heartbeat time alone: the node controller uses it when a kubelet goes quiet.
func (s *Store) SetNodeReady(name string, ready bool) (api.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[name]
	if !ok {
		return api.Node{}, fmt.Errorf("node %q: %w", name, ErrNotFound)
	}

	node.Ready = ready
	node.ResourceVersion = s.nextVersion()
	err := s.put(api.EventModified, kindNodes, node.Name, node)
	if err != nil {
		return api.Node{}, err
	}
	s.nodes[name] = node
	return node, nil
}

// CreateReplicaSet saves a new ReplicaSet. It fails if one with that name
// already exists.
func (s *Store) CreateReplicaSet(rs api.ReplicaSet) (api.ReplicaSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.checkNamespace(rs.Namespace)
	if err != nil {
		return api.ReplicaSet{}, err
	}

	key := api.Key(rs.Namespace, rs.Name)
	_, exists := s.replicaSets[key]
	if exists {
		return api.ReplicaSet{}, fmt.Errorf("replicaset %q already exists in namespace %q: %w", rs.Name, rs.Namespace, ErrConflict)
	}

	s.stampNew(&rs.ObjectMeta)
	err = s.put(api.EventAdded, kindReplicaSets, key, rs)
	if err != nil {
		return api.ReplicaSet{}, err
	}
	s.replicaSets[key] = rs
	return rs, nil
}

// ListReplicaSets returns the ReplicaSets in a namespace, or in all
// namespaces if namespace is "".
func (s *Store) ListReplicaSets(namespace string) []api.ReplicaSet {
	s.mu.Lock()
	defer s.mu.Unlock()

	return inNamespace(s.replicaSets, namespace)
}

// DeleteReplicaSet removes a ReplicaSet. Its pods are left behind for the
// ReplicaSet controller to clean up.
func (s *Store) DeleteReplicaSet(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, name)
	_, ok := s.replicaSets[key]
	if !ok {
		return fmt.Errorf("replicaset %q in namespace %q: %w", name, namespace, ErrNotFound)
	}

	err := s.remove(kindReplicaSets, key, s.replicaSets[key])
	if err != nil {
		return err
	}
	delete(s.replicaSets, key)
	return nil
}

// ScaleReplicaSet changes how many replicas a ReplicaSet wants.
func (s *Store) ScaleReplicaSet(namespace, name string, replicas int) (api.ReplicaSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := api.Key(namespace, name)
	rs, ok := s.replicaSets[key]
	if !ok {
		return api.ReplicaSet{}, fmt.Errorf("replicaset %q in namespace %q: %w", name, namespace, ErrNotFound)
	}

	rs.Replicas = replicas
	rs.ResourceVersion = s.nextVersion()
	err := s.put(api.EventModified, kindReplicaSets, key, rs)
	if err != nil {
		return api.ReplicaSet{}, err
	}
	s.replicaSets[key] = rs
	return rs, nil
}
