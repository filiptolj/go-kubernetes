package store

import (
	"strings"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// watchBuffer is how many events a watcher may fall behind before it is
// dropped.
const watchBuffer = 100

// watcher is one open watch: it gets the changes to the objects of one kind,
// in one namespace or in all of them.
type watcher struct {
	kind      string // such as "pods" or "replicasets"
	namespace string // "" for every namespace
	ch        chan api.WatchEvent[any]
}

// Watch returns a channel that receives an event every time an object of
// kind ("pods", "deployments", ...) changes, in namespace or, if namespace is
// "", anywhere; and a stop function to call when you no longer want events.
//
// Events arrive in the order the changes happened. A watcher that falls more
// than watchBuffer events behind is dropped: its channel is closed, and it
// should list everything again and start a new watch.
func (s *Store) Watch(kind, namespace string) (<-chan api.WatchEvent[any], func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextID
	s.nextID++

	w := &watcher{kind: kind, namespace: namespace, ch: make(chan api.WatchEvent[any], watchBuffer)}
	s.watchers[id] = w

	stop := func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		_, ok := s.watchers[id]
		if ok {
			delete(s.watchers, id)
			close(w.ch)
		}
	}
	return w.ch, stop
}

// broadcast sends an event about obj to everyone watching its kind and
// namespace. kind is the backend's name for the kind ("replicaSets"), and
// key is "namespace/name", or only the name for kinds that don't live in a
// namespace. The caller must hold s.mu.
func (s *Store) broadcast(event api.EventType, kind, key string, obj any) {
	kind = strings.ToLower(kind)
	namespace, _, found := strings.Cut(key, "/")
	if !found {
		namespace = ""
	}

	for id, w := range s.watchers {
		if w.kind != kind || (w.namespace != "" && w.namespace != namespace) {
			continue
		}
		select {
		case w.ch <- api.WatchEvent[any]{Type: event, Object: obj}:
		default:
			// This watcher's buffer is full: it is too slow. Drop it so it
			// can't hold up everyone else. Its channel closes, so it knows.
			delete(s.watchers, id)
			close(w.ch)
		}
	}
}
