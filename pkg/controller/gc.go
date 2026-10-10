package controller

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/client"
)

// GarbageCollector deletes objects whose owners are gone: the pods of a
// deleted ReplicaSet, the ReplicaSets of a deleted Deployment, and so on.
// It goes by the ownerReferences in each object's metadata, like
// Kubernetes' garbage collector, so no controller has to clean up after
// its own deleted objects. Deleting a Deployment deletes its ReplicaSets,
// and then their pods, one level at a time.
type GarbageCollector struct {
	Client    *client.Client
	Informers *Informers    // where to read from; nil: from Client
	Every     time.Duration // how often to look even if nothing changed
}

// Run collects garbage whenever an object changes, until ctx is cancelled.
func (gc *GarbageCollector) Run(ctx context.Context) {
	inf := gc.Informers
	client.RunOnChange(ctx, "garbage collector", gc.Every, gc.collect,
		inf.pods(), inf.replicaSets(), inf.deployments(), inf.jobs(), inf.cronJobs(), inf.daemonSets(), inf.statefulSets())
}

// owners holds the objects that can own others: by kind, then by
// "namespace/name", their UID.
type owners map[string]map[string]string

// listOwners lists every object that can own others, from the informers in
// inf, or from the API server if inf is nil.
func (gc *GarbageCollector) listOwners(inf *Informers) (owners, error) {
	o := make(owners)
	var errs []error
	note := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	sets, err := list(inf.replicaSets(), gc.Client.ListReplicaSets)
	note(err)
	addOwners(o, "ReplicaSet", sets)
	deployments, err := list(inf.deployments(), gc.Client.ListDeployments)
	note(err)
	addOwners(o, "Deployment", deployments)
	jobs, err := list(inf.jobs(), gc.Client.Jobs().List)
	note(err)
	addOwners(o, "Job", jobs)
	cronJobs, err := list(inf.cronJobs(), gc.Client.CronJobs().List)
	note(err)
	addOwners(o, "CronJob", cronJobs)
	daemonSets, err := list(inf.daemonSets(), gc.Client.DaemonSets().List)
	note(err)
	addOwners(o, "DaemonSet", daemonSets)
	statefulSets, err := list(inf.statefulSets(), gc.Client.StatefulSets().List)
	note(err)
	addOwners(o, "StatefulSet", statefulSets)

	return o, errors.Join(errs...)
}

// addOwners records the objects of one kind as possible owners.
func addOwners[T any](o owners, kind string, objects []T) {
	o[kind] = make(map[string]string)
	for i := range objects {
		m := api.MetaOf(&objects[i])
		o[kind][api.Key(m.Namespace, m.Name)] = m.UID
	}
}

// orphaned reports whether every owner an object names is gone. An object
// without owners is never garbage. An owner of a kind the collector doesn't
// know about counts as present, to be safe. An owner that exists under the
// same name with another UID is gone: it was deleted and created again.
func (o owners) orphaned(m api.ObjectMeta) bool {
	if len(m.OwnerReferences) == 0 {
		return false
	}
	for _, ref := range m.OwnerReferences {
		known, ok := o[ref.Kind]
		if !ok {
			return false
		}
		uid, exists := known[api.Key(m.Namespace, ref.Name)]
		if exists && (ref.UID == "" || ref.UID == uid) {
			return false
		}
	}
	return true
}

// dependent is an object that has owners, and how to delete it.
type dependent struct {
	kind   string
	meta   api.ObjectMeta
	delete func() error
}

// listDependents lists every object that may have owners.
func (gc *GarbageCollector) listDependents() []dependent {
	var deps []dependent
	inf := gc.Informers

	pods, _ := list(inf.pods(), gc.Client.ListPods)
	for _, p := range pods {
		deps = append(deps, dependent{"Pod", p.ObjectMeta, func() error { return gc.Client.DeletePod(p.Namespace, p.Name) }})
	}
	sets, _ := list(inf.replicaSets(), gc.Client.ListReplicaSets)
	for _, rs := range sets {
		deps = append(deps, dependent{"ReplicaSet", rs.ObjectMeta, func() error { return gc.Client.DeleteReplicaSet(rs.Namespace, rs.Name) }})
	}
	jobs, _ := list(inf.jobs(), gc.Client.Jobs().List)
	for _, j := range jobs {
		deps = append(deps, dependent{"Job", j.ObjectMeta, func() error { return gc.Client.Jobs().Delete(j.Namespace, j.Name) }})
	}
	return deps
}

// collect deletes every object whose owners are gone.
func (gc *GarbageCollector) collect() {
	cached, err := gc.listOwners(gc.Informers)
	if err != nil {
		log.Printf("garbage collector: %v", err)
		return
	}

	var garbage []dependent
	for _, d := range gc.listDependents() {
		if cached.orphaned(d.meta) {
			garbage = append(garbage, d)
		}
	}
	if len(garbage) == 0 {
		return
	}

	// The informers may lag: an owner created a moment ago may not be in
	// them yet. Before deleting anything, ask the API server itself.
	live := cached
	if gc.Informers != nil {
		live, err = gc.listOwners(nil)
		if err != nil {
			log.Printf("garbage collector: %v", err)
			return
		}
	}

	for _, d := range garbage {
		if !live.orphaned(d.meta) {
			continue
		}
		err := d.delete()
		if err != nil {
			log.Printf("garbage collector: %v", err)
			continue
		}
		ref := d.meta.OwnerReferences[0]
		log.Printf("garbage collector: deleted %s %s: its %s %q is gone",
			d.kind, api.Key(d.meta.Namespace, d.meta.Name), ref.Kind, ref.Name)
	}
}
