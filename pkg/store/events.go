package store

import (
	"fmt"
	"slices"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// maxEvents is how many events the store keeps. Events are a short-lived
// history: they live in memory only, and the oldest are dropped first.
const maxEvents = 1000

// RecordEvent stores an event. If the same event (same object, source,
// reason and message) is already stored, it counts it again instead.
func (s *Store) RecordEvent(e api.Event) api.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for i := range s.events {
		old := &s.events[i]
		if old.Kind == e.Kind && old.Namespace == e.Namespace && old.Name == e.Name && old.Source == e.Source &&
			old.Reason == e.Reason && old.Message == e.Message {
			old.Count++
			old.LastSeen = now
			return *old
		}
	}

	s.nextEventID++
	e.ID = fmt.Sprintf("%d", s.nextEventID)
	e.Count = 1
	e.FirstSeen = now
	e.LastSeen = now

	s.events = append(s.events, e)
	if len(s.events) > maxEvents {
		s.events = slices.Delete(s.events, 0, len(s.events)-maxEvents)
	}
	return e
}

// ListEvents returns events, oldest first. An empty namespace, kind or name
// matches every one, so ListEvents("", "", "") returns all events.
func (s *Store) ListEvents(namespace, kind, name string) []api.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := make([]api.Event, 0)
	for _, e := range s.events {
		if (namespace == "" || e.Namespace == namespace) && (kind == "" || e.Kind == kind) && (name == "" || e.Name == name) {
			events = append(events, e)
		}
	}

	slices.SortStableFunc(events, func(a, b api.Event) int {
		return a.LastSeen.Compare(b.LastSeen)
	})
	return events
}
