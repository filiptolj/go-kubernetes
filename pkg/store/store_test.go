package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// ns is the namespace the tests use.
const ns = api.DefaultNamespace

// meta returns the metadata of an object in ns.
func meta(name string) api.ObjectMeta {
	return metaIn(ns, name)
}

func metaIn(namespace, name string) api.ObjectMeta {
	return api.ObjectMeta{Name: name, Namespace: namespace}
}

// service returns a Service with one port.
func service(name string, port int, selector api.Labels, namespace string) api.Service {
	return api.Service{
		ObjectMeta:  metaIn(namespace, name),
		ServiceSpec: api.ServiceSpec{Selector: selector, Ports: []api.ServicePort{{Port: port}}},
	}
}

func TestCreatePodTwiceConflicts(t *testing.T) {
	s := New()

	_, err := s.CreatePod(api.Pod{ObjectMeta: meta("nginx")})
	if err != nil {
		t.Fatalf("first CreatePod: unexpected error: %v", err)
	}

	_, err = s.CreatePod(api.Pod{ObjectMeta: meta("nginx")})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("second CreatePod: got error %v, want ErrConflict", err)
	}
}

func TestBindPod(t *testing.T) {
	tests := []struct {
		name    string
		pod     string
		node    string
		wantErr error // nil means the bind should work
	}{
		{name: "binds a free pod", pod: "free", node: "node-1", wantErr: nil},
		{name: "unknown pod", pod: "banana", node: "node-1", wantErr: ErrNotFound},
		{name: "unknown node", pod: "free", node: "node-9", wantErr: ErrNotFound},
		{name: "already bound", pod: "taken", node: "node-1", wantErr: ErrConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			s.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-1"}, NodeStatus: api.NodeStatus{Ready: true}})
			s.CreatePod(api.Pod{ObjectMeta: meta("free")})
			s.CreatePod(api.Pod{ObjectMeta: meta("taken"), PodSpec: api.PodSpec{NodeName: "node-1"}})

			pod, err := s.BindPod(ns, tt.pod, tt.node)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("got error %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if pod.NodeName != tt.node {
				t.Errorf("returned pod has NodeName %q, want %q", pod.NodeName, tt.node)
			}

			// The change must be in the store, not only in the returned copy.
			stored, _ := s.GetPod(ns, tt.pod)
			if stored.NodeName != tt.node {
				t.Errorf("stored pod has NodeName %q, want %q", stored.NodeName, tt.node)
			}
		})
	}
}

func TestWatchSeesEveryChange(t *testing.T) {
	s := New()
	s.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-1"}, NodeStatus: api.NodeStatus{Ready: true}})

	events, stop := s.Watch("pods", "")
	defer stop()

	s.CreatePod(api.Pod{ObjectMeta: meta("nginx")})
	s.BindPod(ns, "nginx", "node-1")
	s.SetPodStatus(ns, "nginx", api.PodStatus{Phase: api.PodRunning})
	s.DeletePod(ns, "nginx")

	want := []api.EventType{api.EventAdded, api.EventModified, api.EventModified, api.EventDeleted}
	for i, wantType := range want {
		event := <-events
		if event.Type != wantType || event.Object.(api.Pod).Name != "nginx" {
			t.Errorf("event %d: got %s %q, want %s \"nginx\"", i, event.Type, event.Object.(api.Pod).Name, wantType)
		}
	}
}

func TestStoppedWatcherGetsNoMoreEvents(t *testing.T) {
	s := New()
	events, stop := s.Watch("pods", "")
	stop()
	stop() // stopping twice must be harmless

	s.CreatePod(api.Pod{ObjectMeta: meta("nginx")})

	_, ok := <-events
	if ok {
		t.Error("got an event after stop; want a closed channel")
	}
}

// TestConcurrentCreates creates pods from many goroutines at once. Run it with
// -race: without the mutex, the race detector reports the map being written
// by several goroutines.
func TestConcurrentCreates(t *testing.T) {
	s := New()

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			s.CreatePod(api.Pod{ObjectMeta: meta(fmt.Sprintf("pod-%d", i))})
		})
	}
	wg.Wait()

	got := len(s.ListPods(""))
	if got != 50 {
		t.Errorf("got %d pods, want 50", got)
	}
}

func TestSortedValues(t *testing.T) {
	got := sortedValues(map[string]int{"c": 3, "a": 1, "b": 2})

	want := []int{1, 2, 3}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFinishedPodStaysFinished(t *testing.T) {
	s := New()
	s.CreatePod(api.Pod{ObjectMeta: meta("job")})
	s.SetPodStatus(ns, "job", api.PodStatus{Phase: api.PodRunning, Ready: true})
	s.SetPodStatus(ns, "job", api.PodStatus{Phase: api.PodFailed})

	_, err := s.SetPodStatus(ns, "job", api.PodStatus{Phase: api.PodRunning, Ready: true})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("Running after Failed: got error %v, want ErrConflict", err)
	}

	s.CreatePod(api.Pod{ObjectMeta: meta("done")})
	s.SetPodStatus(ns, "done", api.PodStatus{Phase: api.PodSucceeded})
	_, err = s.SetPodStatus(ns, "done", api.PodStatus{Phase: api.PodFailed})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("Failed after Succeeded: got error %v, want ErrConflict", err)
	}

	pod, _ := s.GetPod(ns, "job")
	if pod.Phase != api.PodFailed || pod.Ready {
		t.Errorf("got phase %s ready %t, want Failed and not ready", pod.Phase, pod.Ready)
	}
	if pod.StartedAt.IsZero() || pod.FinishedAt.IsZero() {
		t.Error("StartedAt and FinishedAt should both be set")
	}
}

func TestEvents(t *testing.T) {
	s := New()
	backoff := api.Event{Kind: "ReplicaSet", Name: "web", Type: api.EventWarning, Reason: "BackOff", Message: "waiting", Source: "replicaset-controller"}

	s.RecordEvent(api.Event{Kind: "Pod", Name: "a", Type: api.EventNormal, Reason: "Scheduled", Source: "scheduler"})
	s.RecordEvent(backoff)
	s.RecordEvent(backoff)
	s.RecordEvent(backoff)

	events := s.ListEvents("", "ReplicaSet", "web")
	if len(events) != 1 {
		t.Fatalf("got %d events for replicaset web, want 1 (repeats are merged)", len(events))
	}
	if events[0].Count != 3 {
		t.Errorf("got count %d, want 3", events[0].Count)
	}
	if n := len(s.ListEvents("", "", "")); n != 2 {
		t.Errorf("got %d events in total, want 2", n)
	}
}

func TestEventsAreCapped(t *testing.T) {
	s := New()
	for i := range maxEvents + 10 {
		s.RecordEvent(api.Event{Kind: "Pod", Name: fmt.Sprintf("p%d", i), Type: api.EventNormal, Reason: "Test"})
	}

	events := s.ListEvents("", "", "")
	if len(events) != maxEvents {
		t.Fatalf("got %d events, want %d", len(events), maxEvents)
	}
	if events[0].Name != "p10" {
		t.Errorf("oldest kept event is %q, want p10 (the 10 oldest dropped)", events[0].Name)
	}
}

func TestCreateFillsInMetadata(t *testing.T) {
	s := New()
	s.CreatePod(api.Pod{ObjectMeta: meta("a")})
	s.CreatePod(api.Pod{ObjectMeta: meta("b")})

	a, _ := s.GetPod(ns, "a")
	b, _ := s.GetPod(ns, "b")
	if a.UID == "" || a.UID == b.UID {
		t.Errorf("got UIDs %q and %q, want two different ones", a.UID, b.UID)
	}
	if a.CreationTimestamp.IsZero() {
		t.Error("CreationTimestamp isn't set")
	}
	if a.ResourceVersion == "" || a.ResourceVersion == b.ResourceVersion {
		t.Errorf("got resourceVersions %q and %q, want two different ones", a.ResourceVersion, b.ResourceVersion)
	}
}

// TestStaleUpdateConflicts is optimistic concurrency: two clients read the
// same version, and only the first one to write it back wins.
func TestStaleUpdateConflicts(t *testing.T) {
	s := New()
	created, err := s.ConfigMaps.Create(api.ConfigMap{ObjectMeta: meta("settings"), Data: map[string]string{"mode": "slow"}})
	if err != nil {
		t.Fatal(err)
	}

	first, second := created, created
	first.Data = map[string]string{"mode": "fast"}
	second.Data = map[string]string{"mode": "medium"}

	updated, err := s.ConfigMaps.Update(first)
	if err != nil {
		t.Fatalf("first update: %v", err)
	}
	if updated.ResourceVersion == created.ResourceVersion {
		t.Error("the update didn't change the resourceVersion")
	}
	if updated.UID != created.UID {
		t.Errorf("the update changed the UID from %q to %q", created.UID, updated.UID)
	}

	_, err = s.ConfigMaps.Update(second)
	if !errors.Is(err, ErrConflict) {
		t.Errorf("update from a stale copy: got error %v, want ErrConflict", err)
	}
	if cm, _ := s.ConfigMaps.Get(ns, "settings"); cm.Data["mode"] != "fast" {
		t.Errorf("got mode %q, want the first update's \"fast\"", cm.Data["mode"])
	}

	// Without a resourceVersion, as from apply, the update always goes through.
	second.ResourceVersion = ""
	if _, err := s.ConfigMaps.Update(second); err != nil {
		t.Errorf("update without a resourceVersion: %v", err)
	}
}

func TestVersionsKeepGrowingAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	open := func() *Store {
		b, err := OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		s, err := Open(b)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	first := open()
	first.CreatePod(api.Pod{ObjectMeta: meta("a")})
	a, _ := first.GetPod(ns, "a")
	first.Close()

	second := open()
	defer second.Close()
	second.CreatePod(api.Pod{ObjectMeta: meta("b")})
	b, _ := second.GetPod(ns, "b")

	av, _ := strconv.Atoi(a.ResourceVersion)
	bv, _ := strconv.Atoi(b.ResourceVersion)
	if bv <= av {
		t.Errorf("after a restart, got version %d, want more than the saved %d", bv, av)
	}
}

func TestWatchOneKindInOneNamespace(t *testing.T) {
	s := New()
	s.CreateNamespace(api.Namespace{ObjectMeta: metaIn("", "dev")})

	events, stop := s.Watch("configmaps", "dev")
	defer stop()

	s.ConfigMaps.Create(api.ConfigMap{ObjectMeta: meta("not-in-dev")})
	s.CreatePod(api.Pod{ObjectMeta: metaIn("dev", "not-a-configmap")})
	created, _ := s.ConfigMaps.Create(api.ConfigMap{ObjectMeta: metaIn("dev", "settings")})
	s.ConfigMaps.Delete("dev", "settings")

	for _, want := range []api.EventType{api.EventAdded, api.EventDeleted} {
		event := <-events
		cm, ok := event.Object.(api.ConfigMap)
		if event.Type != want || !ok || cm.Name != "settings" {
			t.Fatalf("got %s %+v, want %s of configmap settings", event.Type, event.Object, want)
		}
		if cm.ResourceVersion != created.ResourceVersion {
			t.Errorf("%s: got resourceVersion %q, want %q", want, cm.ResourceVersion, created.ResourceVersion)
		}
	}
	select {
	case event := <-events:
		t.Errorf("got an extra event %s %+v", event.Type, event.Object)
	default:
	}
}

func TestFinalizers(t *testing.T) {
	s := New()
	cm := api.ConfigMap{ObjectMeta: meta("settings")}
	cm.Finalizers = []string{"example.com/backup"}
	s.ConfigMaps.Create(cm)

	// Deleting only marks it: the finalizer's work isn't done yet.
	if err := s.ConfigMaps.Delete(ns, "settings"); err != nil {
		t.Fatal(err)
	}
	marked, ok := s.ConfigMaps.Get(ns, "settings")
	if !ok || !marked.Terminating() {
		t.Fatalf("after delete: got %+v (found: %t), want it still there, terminating", marked.ObjectMeta, ok)
	}

	// Clients can't take the mark back.
	marked.DeletionTimestamp = nil
	marked.Data = map[string]string{"changed": "yes"}
	updated, _ := s.ConfigMaps.Update(marked)
	if !updated.Terminating() {
		t.Error("an update cleared the deletion mark")
	}

	// Removing the last finalizer lets it go.
	updated.Finalizers = nil
	if _, err := s.ConfigMaps.Update(updated); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.ConfigMaps.Get(ns, "settings"); ok {
		t.Error("still there after its last finalizer was removed")
	}
}

func TestDeletePodGracefully(t *testing.T) {
	s := New()
	s.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-1"}, NodeStatus: api.NodeStatus{Ready: true}})
	s.CreatePod(api.Pod{ObjectMeta: meta("unscheduled")})
	s.CreatePod(api.Pod{ObjectMeta: meta("running")})
	s.BindPod(ns, "running", "node-1")
	s.SetPodStatus(ns, "running", api.PodStatus{Phase: api.PodRunning})

	// No node: nothing to stop, so it goes at once.
	if _, removed, _ := s.DeletePodGracefully(ns, "unscheduled", -1); !removed {
		t.Error("a pod without a node wasn't removed at once")
	}

	// On a node: marked, with the pod's default grace period.
	pod, removed, _ := s.DeletePodGracefully(ns, "running", -1)
	if removed || !pod.Terminating() || *pod.DeletionGracePeriodSeconds != api.DefaultTerminationGracePeriod {
		t.Fatalf("got removed %t, %+v; want it terminating with %ds", removed, pod.ObjectMeta, api.DefaultTerminationGracePeriod)
	}

	// Asking again with less time shortens it; with 0, it goes.
	pod, _, _ = s.DeletePodGracefully(ns, "running", 5)
	if *pod.DeletionGracePeriodSeconds != 5 {
		t.Errorf("got grace %d, want it shortened to 5", *pod.DeletionGracePeriodSeconds)
	}
	if _, removed, _ := s.DeletePodGracefully(ns, "running", 0); !removed {
		t.Error("grace 0 didn't remove the pod")
	}
}
