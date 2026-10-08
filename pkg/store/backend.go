package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// Backend is where a Store keeps its objects so they survive restarts. Each
// object is saved as JSON under its kind (such as "pods") and its key: its
// name, or "namespace/name" for objects in a namespace.
//
// The Store only calls a Backend while holding its lock, so a Backend
// doesn't need a lock of its own.
type Backend interface {
	// LoadAll returns every saved object: by kind, then by key.
	LoadAll() (map[string]map[string][]byte, error)

	// Put saves one object, replacing it if it already exists.
	Put(kind, name string, data []byte) error

	// Delete removes one object. Deleting one that doesn't exist is fine.
	Delete(kind, name string) error

	// Close releases the Backend's resources, such as a network connection.
	Close() error
}

// The kinds of objects the Store saves.
const (
	kindPods        = "pods"
	kindNodes       = "nodes"
	kindReplicaSets = "replicaSets"
	kindDeployments = "deployments"
	kindServices    = "services"
	kindNamespaces  = "namespaces"
)

// ErrOldFormat means the saved data was written by an older version of
// minik8s, before objects had Kubernetes' shape (metadata, spec, status).
var ErrOldFormat = errors.New("the saved cluster was written by an older version of minik8s and can't be read; " +
	"delete it to start a new cluster (the data file, or the /minik8s/ keys in etcd)")

// Open returns a Store that saves every change in b, starting with whatever
// b already holds.
func Open(b Backend) (*Store, error) {
	s := New()
	s.backend = b

	objects, err := b.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("load saved objects: %w", err)
	}

	err = loadInto(s.pods, objects[kindPods])
	if err == nil {
		err = loadInto(s.nodes, objects[kindNodes])
	}
	if err == nil {
		err = loadInto(s.replicaSets, objects[kindReplicaSets])
	}
	if err == nil {
		err = loadInto(s.deployments, objects[kindDeployments])
	}
	if err == nil {
		err = loadInto(s.services, objects[kindServices])
	}
	if err == nil {
		err = loadInto(s.namespaces, objects[kindNamespaces])
	}
	for _, r := range s.resources {
		if err == nil {
			err = r.load(objects[r.backendKind()])
		}
	}
	if err != nil {
		return nil, fmt.Errorf("load saved objects: %w", err)
	}

	// New resourceVersions must be higher than every saved one.
	seeVersions(s, s.pods)
	seeVersions(s, s.nodes)
	seeVersions(s, s.replicaSets)
	seeVersions(s, s.deployments)
	seeVersions(s, s.services)
	seeVersions(s, s.namespaces)

	// The default namespace always exists, also in the backend.
	if objects[kindNamespaces][api.DefaultNamespace] == nil {
		err = s.put(api.EventAdded, kindNamespaces, api.DefaultNamespace, s.namespaces[api.DefaultNamespace])
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// seeVersions moves the store's version counter past every object in m.
func seeVersions[T any, PT object[T]](s *Store, m map[string]T) {
	for _, obj := range m {
		s.seeVersion(PT(&obj).GetObjectMeta().ResourceVersion)
	}
}

// loadInto decodes saved objects of one kind into m. T is the object's type,
// such as api.Pod: Go works it out from the map passed in.
func loadInto[T any](m map[string]T, objects map[string][]byte) error {
	for name, data := range objects {
		// Objects in the current format have their name under "metadata".
		var shape struct {
			Metadata *struct{} `json:"metadata"`
		}
		err := json.Unmarshal(data, &shape)
		if err == nil && shape.Metadata == nil {
			return ErrOldFormat
		}

		var obj T
		err = json.Unmarshal(data, &obj)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		m[name] = obj
	}
	return nil
}

// put saves obj in the backend, and tells the watchers of its kind what
// happened to it. The caller must hold s.mu.
func (s *Store) put(event api.EventType, kind, name string, obj any) error {
	if s.backend == nil {
		s.broadcast(event, kind, name, obj)
		return nil
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("save %s/%s: %w", kind, name, err)
	}

	err = s.backend.Put(kind, name, data)
	if err != nil {
		return fmt.Errorf("save %s/%s: %w", kind, name, err)
	}
	s.broadcast(event, kind, name, obj)
	return nil
}

// remove deletes an object from the backend, and tells the watchers of its
// kind, sending them obj as it was last. The caller must hold s.mu.
func (s *Store) remove(kind, name string, obj any) error {
	if s.backend != nil {
		err := s.backend.Delete(kind, name)
		if err != nil {
			return fmt.Errorf("delete %s/%s: %w", kind, name, err)
		}
	}
	s.broadcast(api.EventDeleted, kind, name, obj)
	return nil
}

// Close closes the store's backend, if it has one.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.backend == nil {
		return nil
	}
	return s.backend.Close()
}

// sortedValues returns the values of a map, ordered by their keys. It works
// for any type of value: V is a type parameter, filled in by the caller.
func sortedValues[V any](m map[string]V) []V {
	values := make([]V, 0, len(m))
	for _, key := range slices.Sorted(maps.Keys(m)) {
		values = append(values, m[key])
	}
	return values
}
