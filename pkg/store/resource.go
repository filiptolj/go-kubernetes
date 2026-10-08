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
	kind    string                   // the kind in the backend, such as "jobs"
	label   string                   // the kind in messages, such as "job"
	meta    func(*T) *api.ObjectMeta // where an object keeps its name, namespace and so on
	objects map[string]T             // by "namespace/name"
}

// object is a constraint: it says what newResource needs from a kind. PT
// must be a pointer to T (*T), and must have a GetObjectMeta method. Every
// kind gets that method by embedding api.ObjectMeta, so for T = api.Job, PT
// is *api.Job, and Go works that out by itself.
type object[T any] interface {
	*T
	GetObjectMeta() *api.ObjectMeta
}

// newResource returns a Resource of the store s, and registers it, so that
// the store loads it, saves it, and empties it when a namespace is deleted.
func newResource[T any, PT object[T]](s *Store, kind, label string) *Resource[T] {
	r := &Resource[T]{
		s:     s,
		kind:  kind,
		label: label,
		// PT(o) converts the *T to PT, which has the GetObjectMeta method.
		meta:    func(o *T) *api.ObjectMeta { return PT(o).GetObjectMeta() },
		objects: make(map[string]T),
	}
	s.resources = append(s.resources, r)
	return r
}

// Meta returns the metadata of obj. Changing it through the returned pointer
// changes obj.
func (r *Resource[T]) Meta(obj *T) *api.ObjectMeta {
	return r.meta(obj)
}

// Create saves a new object. It fails if the object's namespace doesn't
// exist, or if an object of this kind with the same name is already in it.
// It returns the object as stored, with its UID and resourceVersion.
func (r *Resource[T]) Create(obj T) (T, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	m := r.meta(&obj)
	err := r.s.checkNamespace(m.Namespace)
	if err != nil {
		return obj, err
	}

	key := api.Key(m.Namespace, m.Name)
	_, exists := r.objects[key]
	if exists {
		return obj, fmt.Errorf("%s %q already exists in namespace %q: %w", r.label, m.Name, m.Namespace, ErrConflict)
	}

	r.s.stampNew(m)
	err = r.s.put(api.EventAdded, r.kind, key, obj)
	if err != nil {
		return obj, err
	}
	r.objects[key] = obj
	return obj, nil
}

// Update replaces an existing object. If obj carries a resourceVersion, it
// must be the stored one: otherwise obj is based on stale data, and the
// update is refused with ErrConflict.
func (r *Resource[T]) Update(obj T) (T, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	m := r.meta(&obj)
	key := api.Key(m.Namespace, m.Name)
	stored, ok := r.objects[key]
	if !ok {
		return obj, fmt.Errorf("%s %q in namespace %q: %w", r.label, m.Name, m.Namespace, ErrNotFound)
	}

	err := r.s.stampUpdate(m, *r.meta(&stored))
	if err != nil {
		return obj, fmt.Errorf("%s %q: %w", r.label, m.Name, err)
	}
	err = r.s.put(api.EventModified, r.kind, key, obj)
	if err != nil {
		return obj, err
	}
	r.objects[key] = obj
	return obj, nil
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

	err := r.s.remove(r.kind, key, r.objects[key])
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
	err := loadInto(r.objects, objects)
	if err != nil {
		return err
	}
	for key, obj := range r.objects {
		r.s.seeVersion(r.meta(&obj).ResourceVersion)
		r.objects[key] = obj
	}
	return nil
}

// deleteNamespace removes every object of this kind in a namespace. The
// caller must hold s.mu.
func (r *Resource[T]) deleteNamespace(namespace string) error {
	return deleteAllIn(r.s, r.kind, r.objects, namespace)
}
