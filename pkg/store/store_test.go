package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// ns is the namespace the tests use.
const ns = api.DefaultNamespace

func TestCreatePodTwiceConflicts(t *testing.T) {
	s := New()

	err := s.CreatePod(api.Pod{Namespace: ns, Name: "nginx"})
	if err != nil {
		t.Fatalf("first CreatePod: unexpected error: %v", err)
	}

	err = s.CreatePod(api.Pod{Namespace: ns, Name: "nginx"})
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
			s.PutNode(api.Node{Name: "node-1", Ready: true})
			s.CreatePod(api.Pod{Namespace: ns, Name: "free"})
			s.CreatePod(api.Pod{Namespace: ns, Name: "taken", NodeName: "node-1"})

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
	s.PutNode(api.Node{Name: "node-1", Ready: true})

	events, stop := s.WatchPods()
	defer stop()

	s.CreatePod(api.Pod{Namespace: ns, Name: "nginx"})
	s.BindPod(ns, "nginx", "node-1")
	s.SetPodStatus(ns, "nginx", api.PodStatus{Phase: api.PodRunning})
	s.DeletePod(ns, "nginx")

	want := []api.EventType{api.EventAdded, api.EventModified, api.EventModified, api.EventDeleted}
	for i, wantType := range want {
		event := <-events
		if event.Type != wantType || event.Pod.Name != "nginx" {
			t.Errorf("event %d: got %s %q, want %s \"nginx\"", i, event.Type, event.Pod.Name, wantType)
		}
	}
}

func TestStoppedWatcherGetsNoMoreEvents(t *testing.T) {
	s := New()
	events, stop := s.WatchPods()
	stop()
	stop() // stopping twice must be harmless

	s.CreatePod(api.Pod{Namespace: ns, Name: "nginx"})

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
			s.CreatePod(api.Pod{Namespace: ns, Name: fmt.Sprintf("pod-%d", i)})
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
	s.CreatePod(api.Pod{Namespace: ns, Name: "job"})
	s.SetPodStatus(ns, "job", api.PodStatus{Phase: api.PodRunning, Ready: true})
	s.SetPodStatus(ns, "job", api.PodStatus{Phase: api.PodFailed})

	_, err := s.SetPodStatus(ns, "job", api.PodStatus{Phase: api.PodRunning, Ready: true})
	if !errors.Is(err, ErrConflict) {
		t.Errorf("Running after Failed: got error %v, want ErrConflict", err)
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
