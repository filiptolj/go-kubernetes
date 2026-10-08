package store

import (
	"fmt"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// Resource stores one kind of namespaced object, such as Jobs. It is
// generic: T is the object's type, such as api.Job, and the same code serves
// every kind. Its methods are safe to call from many goroutines at once.
type Resource[T any] struct {
	s       *Store
	kind    string             // the kind in the backend, such as "jobs"
	label   string             // the kind in messages, such as "job"
	meta    func(*T) *api.Meta // where an object keeps its name and namespace
	objects map[string]T       // by "namespace/name"
}

// newResource returns a Resource of the store s, and registers it, so that
// the store loads it, saves it, and empties it when a namespace is deleted.
func newResource[T any](s *Store, kind, label string, meta func(*T) *api.Meta) *Resource[T] {
	r := &Resource[T]{s: s, kind: kind, label: label, meta: meta, objects: make(map[string]T)}
	s.resources = append(s.resources, r)
	return r
}

// Meta returns the name and namespace of obj. Changing them through the
// returned pointer changes obj.
func (r *Resource[T]) Meta(obj *T) *api.Meta {
	return r.meta(obj)
}

// Create saves a new object. It fails if the object's namespace doesn't
// exist, or if an object of this kind with the same name is already in it.
func (r *Resource[T]) Create(obj T) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	m := r.meta(&obj)
	err := r.s.checkNamespace(m.Namespace)
	if err != nil {
		return err
	}

	key := api.Key(m.Namespace, m.Name)
	_, exists := r.objects[key]
	if exists {
		return fmt.Errorf("%s %q already exists in namespace %q: %w", r.label, m.Name, m.Namespace, ErrConflict)
	}

	err = r.s.put(r.kind, key, obj)
	if err != nil {
		return err
	}
	r.objects[key] = obj
	return nil
}

// Update replaces an existing object.
func (r *Resource[T]) Update(obj T) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	m := r.meta(&obj)
	key := api.Key(m.Namespace, m.Name)
	_, ok := r.objects[key]
	if !ok {
		return fmt.Errorf("%s %q in namespace %q: %w", r.label, m.Name, m.Namespace, ErrNotFound)
	}

	err := r.s.put(r.kind, key, obj)
	if err != nil {
		return err
	}
	r.objects[key] = obj
	return nil
}

// Get returns one object. The bool is false if it doesn't exist.
func (r *Resource[T]) Get(namespace, name string) (T, bool) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	obj, ok := r.objects[api.Key(namespace, name)]
	return obj, ok
}

// List returns the objects in a namespace, or in all namespaces if
// namespace is "", sorted by namespace and name.
func (r *Resource[T]) List(namespace string) []T {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	return inNamespace(r.objects, namespace)
}

// Delete removes one object.
func (r *Resource[T]) Delete(namespace, name string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	key := api.Key(namespace, name)
	_, ok := r.objects[key]
	if !ok {
		return fmt.Errorf("%s %q in namespace %q: %w", r.label, name, namespace, ErrNotFound)
	}

	err := r.s.remove(r.kind, key)
	if err != nil {
		return err
	}
	delete(r.objects, key)
	return nil
}

// resource is what the Store needs from every Resource, whatever its T. A
// slice of this interface can hold a *Resource[api.Job] next to a
// *Resource[api.Secret], which a slice of one generic type couldn't.
type resource interface {
	backendKind() string
	load(objects map[string][]byte) error
	deleteNamespace(namespace string) error
}

func (r *Resource[T]) backendKind() string {
	return r.kind
}

// load decodes the saved objects of this kind. The caller must hold s.mu.
func (r *Resource[T]) load(objects map[string][]byte) error {
	return loadInto(r.objects, objects)
}

// deleteNamespace removes every object of this kind in a namespace. The
// caller must hold s.mu.
func (r *Resource[T]) deleteNamespace(namespace string) error {
	return deleteAllIn(r.s, r.kind, r.objects, namespace)
}
