package controller

import (
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// expectationTimeout is how long a controller waits to see its own change
// in its informers before it gives up and looks again anyway.
const expectationTimeout = 30 * time.Second

// expectations remember the changes a controller made but hasn't seen in its
// informers yet, for each object it manages.
//
// They solve a problem that comes with informers: a ReplicaSet controller
// creates 3 pods, and a moment later the informer for ReplicaSets wakes it
// up again. The informer for pods may not have received the new pods yet, so
// the controller would count 0 and create 3 more. With expectations, it
// remembers "I expect to see these 3 pods" and leaves the ReplicaSet alone
// until it has. Kubernetes' controllers do the same.
//
// A controller calls check at the start of each pass, before it reads its
// informers, and then asks satisfied about each object. The order matters:
// informers only ever move forward, so whatever the controller reads after
// check is at least as new as what check saw. Checking after reading could
// see a change arrive in between, and then act on the older copy it read.
//
// The zero value is ready to use.
type expectations struct {
	mu      sync.Mutex
	pending map[string][]expectation // by the managed object's "namespace/name"
	waiting map[string]bool          // objects still waiting at the last check
}

type expectation struct {
	seen  func() bool // whether the informers show the change yet
	until time.Time   // give up after this
}

// expect records that the controller changed something for owner, and that
// seen reports when the change has arrived in the informers.
func (e *expectations) expect(owner string, seen func() bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.pending == nil {
		e.pending = make(map[string][]expectation)
	}
	e.pending[owner] = append(e.pending[owner], expectation{seen, time.Now().Add(expectationTimeout)})
}

// check looks at which expected changes have arrived in the informers (or
// waited too long). satisfied answers from this check until the next one.
func (e *expectations) check() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.waiting = make(map[string]bool)
	for owner, list := range e.pending {
		var still []expectation
		for _, x := range list {
			if !x.seen() && time.Now().Before(x.until) {
				still = append(still, x)
			}
		}
		if len(still) == 0 {
			delete(e.pending, owner)
			continue
		}
		e.pending[owner] = still
		e.waiting[owner] = true
	}
}

// satisfied reports whether, at the last check, every change made for owner
// had arrived, so the controller can trust its informers about owner.
func (e *expectations) satisfied(owner string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.waiting[owner]
}

// expectPresent expects the informer to show the object namespace/name,
// after the controller created it. Without an informer, there's nothing to
// wait for.
func expectPresent[T any](e *expectations, inf *client.Informer[T], owner, namespace, name string) {
	if inf == nil {
		return
	}
	e.expect(owner, func() bool {
		_, ok := inf.Get(namespace, name)
		return ok
	})
}

// expectGone expects the informer to no longer show the object
// namespace/name, after the controller deleted it.
func expectGone[T any](e *expectations, inf *client.Informer[T], owner, namespace, name string) {
	if inf == nil {
		return
	}
	e.expect(owner, func() bool {
		_, ok := inf.Get(namespace, name)
		return !ok
	})
}

// expectNewVersion expects the informer to show a newer version of the
// object namespace/name than oldVersion (or no object at all), after the
// controller changed it. Until then the copy in the informer is out of date:
// changing it again would be refused with a conflict, since its
// resourceVersion is the old one.
func expectNewVersion[T any](e *expectations, inf *client.Informer[T], owner, namespace, name, oldVersion string) {
	if inf == nil {
		return
	}
	e.expect(owner, func() bool {
		obj, ok := inf.Get(namespace, name)
		return !ok || api.MetaOf(&obj).ResourceVersion != oldVersion
	})
}
