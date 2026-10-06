package store

import (
	"encoding/json"
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

// Open returns a Store that saves every change in b, starting with whatever
// b already holds. Objects saved before namespaces existed are moved into
// the default namespace.
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
	if err != nil {
		return nil, fmt.Errorf("load saved objects: %w", err)
	}

	// The default namespace always exists, also in the backend.
	if objects[kindNamespaces][api.DefaultNamespace] == nil {
		err = s.put(kindNamespaces, api.DefaultNamespace, s.namespaces[api.DefaultNamespace])
		if err != nil {
			return nil, err
		}
	}

	err = s.migrate()
	if err != nil {
		return nil, fmt.Errorf("move objects into the default namespace: %w", err)
	}
	return s, nil
}

// migrate moves objects saved before namespaces existed into the default
// namespace: their key changes from "name" to "default/name". Changing how
// saved data looks, and converting the old data, is called a migration.
func (s *Store) migrate() error {
	for _, key := range slices.Collect(maps.Keys(s.pods)) {
		pod := s.pods[key]
		if pod.Namespace == "" {
			pod.Namespace = api.DefaultNamespace
			err := moveKey(s, kindPods, s.pods, key, api.Key(pod.Namespace, pod.Name), pod)
			if err != nil {
				return err
			}
		}
	}
	for _, key := range slices.Collect(maps.Keys(s.replicaSets)) {
		rs := s.replicaSets[key]
		if rs.Namespace == "" {
			rs.Namespace = api.DefaultNamespace
			err := moveKey(s, kindReplicaSets, s.replicaSets, key, api.Key(rs.Namespace, rs.Name), rs)
			if err != nil {
				return err
			}
		}
	}
	for _, key := range slices.Collect(maps.Keys(s.deployments)) {
		d := s.deployments[key]
		if d.Namespace == "" {
			d.Namespace = api.DefaultNamespace
			err := moveKey(s, kindDeployments, s.deployments, key, api.Key(d.Namespace, d.Name), d)
			if err != nil {
				return err
			}
		}
	}
	for _, key := range slices.Collect(maps.Keys(s.services)) {
		svc := s.services[key]
		if svc.Namespace == "" {
			svc.Namespace = api.DefaultNamespace
			err := moveKey(s, kindServices, s.services, key, api.Key(svc.Namespace, svc.Name), svc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// moveKey saves obj under its new key and removes the old one, both in the
// backend and in m.
func moveKey[T any](s *Store, kind string, m map[string]T, oldKey, newKey string, obj T) error {
	err := s.put(kind, newKey, obj)
	if err != nil {
		return err
	}
	err = s.remove(kind, oldKey)
	if err != nil {
		return err
	}
	delete(m, oldKey)
	m[newKey] = obj
	return nil
}

// loadInto decodes saved objects of one kind into m. T is the object's type,
// such as api.Pod: Go works it out from the map passed in.
func loadInto[T any](m map[string]T, objects map[string][]byte) error {
	for name, data := range objects {
		var obj T
		err := json.Unmarshal(data, &obj)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		m[name] = obj
	}
	return nil
}

// put saves obj in the backend. The caller must hold s.mu.
func (s *Store) put(kind, name string, obj any) error {
	if s.backend == nil {
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
	return nil
}

// remove deletes an object from the backend. The caller must hold s.mu.
func (s *Store) remove(kind, name string) error {
	if s.backend == nil {
		return nil
	}

	err := s.backend.Delete(kind, name)
	if err != nil {
		return fmt.Errorf("delete %s/%s: %w", kind, name, err)
	}
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
