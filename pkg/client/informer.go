package client

import (
	"context"
	"errors"
	"log"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// Informer keeps a copy of every object of one kind, in every namespace, up
// to date. Controllers read from it instead of asking the API server for
// everything each time they look.
//
// It works the way Kubernetes' informers do: start a watch, list everything,
// then apply each change the watch reports. If the watch breaks, it starts
// over. The copy can lag a little behind the API server, so code that
// changes something and then reads it back may not see its own change yet.
type Informer[T any] struct {
	client *Client
	plural string // such as "pods"

	mu      sync.RWMutex
	objects map[string]T // by "namespace/name", or name for nodes and namespaces
	synced  bool
	notify  []chan<- struct{}
}

// NewInformer returns an Informer for one kind, given its plural as in
// URLs. T must be that kind's type: NewInformer[api.Pod](c, "pods").
// Nothing happens until Run is called.
func NewInformer[T any](c *Client, plural string) *Informer[T] {
	return &Informer[T]{client: c, plural: plural, objects: make(map[string]T)}
}

// Run keeps the copy up to date until ctx is cancelled.
func (inf *Informer[T]) Run(ctx context.Context) {
	for {
		err := inf.listAndWatch(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Printf("informer for %s: %v (starting over in %s)", inf.plural, err, RetryDelay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(RetryDelay):
		}
	}
}

// listAndWatch fills the copy from a list, then applies changes until the
// watch ends.
func (inf *Informer[T]) listAndWatch(ctx context.Context) error {
	path := listPath("", inf.plural)

	// Watch first and list second. Changes made while the list is on its
	// way wait in the watch, so none are missed; the ones already in the
	// list are recognized by their resourceVersion and skipped.
	events, err := Watch[T](ctx, inf.client, path)
	if err != nil {
		return err
	}
	var list []T
	err = inf.client.get(path, &list)
	if err != nil {
		return err
	}

	inf.mu.Lock()
	inf.objects = make(map[string]T, len(list))
	for _, obj := range list {
		inf.objects[key(&obj)] = obj
	}
	inf.synced = true
	inf.mu.Unlock()
	inf.changed()

	for event := range events {
		if inf.apply(event) {
			inf.changed()
		}
	}
	return errors.New("the watch ended")
}

// apply makes one change to the copy. It returns false if the change was
// older than what the copy already has.
func (inf *Informer[T]) apply(event api.WatchEvent[T]) bool {
	inf.mu.Lock()
	defer inf.mu.Unlock()

	k := key(&event.Object)
	version := api.MetaOf(&event.Object).ResourceVersion
	have, ok := inf.objects[k]
	haveVersion := api.MetaOf(&have).ResourceVersion

	switch event.Type {
	case api.EventDeleted:
		if !ok || newer(haveVersion, version) {
			return false // already gone, or created again since
		}
		delete(inf.objects, k)
	default:
		if ok && !newer(version, haveVersion) {
			return false // the list already had this version
		}
		inf.objects[k] = event.Object
	}
	return true
}

// changed tells everyone who asked with Notify that something changed.
func (inf *Informer[T]) changed() {
	inf.mu.RLock()
	defer inf.mu.RUnlock()

	for _, ch := range inf.notify {
		// Don't wait: if ch already holds a signal, its reader will look
		// at everything anyway, so one signal is as good as two.
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Notify asks the informer to send to ch after each change. Make ch with a
// buffer of 1: the informer never waits for a send. (Notify and WaitForSync
// do nothing on a nil *Informer, so code can treat "no informer" like one
// that never changes.)
func (inf *Informer[T]) Notify(ch chan<- struct{}) {
	if inf == nil {
		return
	}
	inf.mu.Lock()
	defer inf.mu.Unlock()
	inf.notify = append(inf.notify, ch)
}

// Synced reports whether the copy has been filled from a first list.
func (inf *Informer[T]) Synced() bool {
	inf.mu.RLock()
	defer inf.mu.RUnlock()
	return inf.synced
}

// WaitForSync waits until the copy has been filled from a first list. It
// returns false if ctx was cancelled first.
func (inf *Informer[T]) WaitForSync(ctx context.Context) bool {
	if inf == nil {
		return true
	}
	for !inf.Synced() {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(20 * time.Millisecond):
		}
	}
	return true
}

// List returns the objects in a namespace, or in all of them if namespace is
// "", sorted by namespace and name.
func (inf *Informer[T]) List(namespace string) []T {
	inf.mu.RLock()
	defer inf.mu.RUnlock()

	var list []T
	for _, k := range slices.Sorted(maps.Keys(inf.objects)) {
		obj := inf.objects[k]
		if namespace == "" || api.MetaOf(&obj).Namespace == namespace {
			list = append(list, obj)
		}
	}
	return list
}

// Get returns one object from the copy.
func (inf *Informer[T]) Get(namespace, name string) (T, bool) {
	inf.mu.RLock()
	defer inf.mu.RUnlock()

	k := name
	if namespace != "" {
		k = api.Key(namespace, name)
	}
	obj, ok := inf.objects[k]
	return obj, ok
}

// key returns where an object goes in an informer's map.
func key(obj any) string {
	m := api.MetaOf(obj)
	if m.Namespace == "" {
		return m.Name
	}
	return api.Key(m.Namespace, m.Name)
}

// newer reports whether resourceVersion a is newer than b. Versions are
// numbers that only grow, so a larger one is newer. Comparing them as text
// would get "10" < "9" wrong.
func newer(a, b string) bool {
	x, _ := strconv.ParseUint(a, 10, 64)
	y, _ := strconv.ParseUint(b, 10, 64)
	return x > y
}

// Source is something RunOnChange can wait for and be woken up by: an
// Informer of any kind.
type Source interface {
	Notify(ch chan<- struct{})
	WaitForSync(ctx context.Context) bool
}

// RunOnChange calls fn once the sources have their first list, then
// every time one of them changes, until ctx is cancelled. It also calls it
// every resync, even if nothing changed, for anything that depends on time,
// such as a backoff running out.
//
// Changes that arrive while fn runs are merged into one: fn looks at
// everything each time, so once is enough.
func RunOnChange(ctx context.Context, name string, resync time.Duration, fn func(), sources ...Source) {
	changed := make(chan struct{}, 1)
	for _, s := range sources {
		s.Notify(changed)
	}
	for _, s := range sources {
		if !s.WaitForSync(ctx) {
			return
		}
	}
	log.Printf("%s: caches filled, starting", name)

	ticker := time.NewTicker(resync)
	defer ticker.Stop()

	fn()
	for {
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-ticker.C:
		}
		fn()
	}
}
