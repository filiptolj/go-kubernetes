package store

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// CreateNamespace saves a new namespace.
func (s *Store) CreateNamespace(ns api.Namespace) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.namespaces[ns.Name]
	if exists {
		return fmt.Errorf("namespace %q already exists: %w", ns.Name, ErrConflict)
	}

	err := s.put(kindNamespaces, ns.Name, ns)
	if err != nil {
		return err
	}
	s.namespaces[ns.Name] = ns
	return nil
}

// ListNamespaces returns every namespace, sorted by name.
func (s *Store) ListNamespaces() []api.Namespace {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sortedValues(s.namespaces)
}

// DeleteNamespace removes a namespace and everything in it: its Services,
// Deployments, ReplicaSets, pods and events. Watchers see the pods deleted,
// so kubelets stop them. The default namespace can't be deleted.
func (s *Store) DeleteNamespace(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if name == api.DefaultNamespace {
		return fmt.Errorf("the %q namespace can't be deleted: %w", name, ErrConflict)
	}
	_, ok := s.namespaces[name]
	if !ok {
		return fmt.Errorf("namespace %q: %w", name, ErrNotFound)
	}

	// Delete from the top down: first what creates pods, then the pods. If a
	// step fails halfway, deleting the namespace again finishes the job.
	err := deleteAllIn(s, kindServices, s.services, name)
	if err == nil {
		err = deleteAllIn(s, kindDeployments, s.deployments, name)
	}
	if err == nil {
		err = deleteAllIn(s, kindReplicaSets, s.replicaSets, name)
	}
	if err != nil {
		return err
	}

	for _, key := range keysIn(s.pods, name) {
		err := s.deletePod(key, s.pods[key])
		if err != nil {
			return err
		}
	}

	err = s.remove(kindNamespaces, name)
	if err != nil {
		return err
	}
	delete(s.namespaces, name)

	s.events = slices.DeleteFunc(s.events, func(e api.Event) bool {
		return e.Namespace == name
	})
	return nil
}

// checkNamespace returns an error if the namespace doesn't exist. The caller
// must hold s.mu.
func (s *Store) checkNamespace(name string) error {
	_, ok := s.namespaces[name]
	if !ok {
		return fmt.Errorf("namespace %q: %w", name, ErrNotFound)
	}
	return nil
}

// deleteAllIn removes every object of one kind in a namespace, from the
// backend and from m. The caller must hold s.mu.
//
// It is a function, not a method, because Go methods can't have type
// parameters of their own.
func deleteAllIn[T any](s *Store, kind string, m map[string]T, namespace string) error {
	for _, key := range keysIn(m, namespace) {
		err := s.remove(kind, key)
		if err != nil {
			return err
		}
		delete(m, key)
	}
	return nil
}

// keysIn returns the sorted keys of the objects in m that live in a
// namespace, or of every object if namespace is "". Keys look like
// "namespace/name", so this is a prefix match.
func keysIn[T any](m map[string]T, namespace string) []string {
	var keys []string
	for _, key := range slices.Sorted(maps.Keys(m)) {
		if namespace == "" || strings.HasPrefix(key, namespace+"/") {
			keys = append(keys, key)
		}
	}
	return keys
}

// inNamespace returns the objects in m that live in a namespace, or every
// object if namespace is "", sorted by namespace and name.
func inNamespace[T any](m map[string]T, namespace string) []T {
	objects := make([]T, 0) // not nil: an empty list must encode to [] in JSON
	for _, key := range keysIn(m, namespace) {
		objects = append(objects, m[key])
	}
	return objects
}
