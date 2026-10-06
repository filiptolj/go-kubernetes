package apiserver

import (
	"net/http"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// handleListEvents returns events as JSON. The query can narrow them down:
// ?namespace=dev for one namespace, plus &kind=Pod&name=web-x7k2p for one object.
func (s *server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	writeJSON(w, http.StatusOK, s.store.ListEvents(query.Get("namespace"), query.Get("kind"), query.Get("name")))
}

// handleRecordEvent stores an event sent by a component.
func (s *server) handleRecordEvent(w http.ResponseWriter, r *http.Request) {
	var e api.Event
	if !decode(w, r, "event", &e) {
		return
	}

	switch {
	case e.Kind == "" || e.Name == "":
		http.Error(w, "an event needs the kind and name of its object", http.StatusBadRequest)
		return
	case e.Reason == "":
		http.Error(w, "an event needs a reason", http.StatusBadRequest)
		return
	case e.Type != api.EventNormal && e.Type != api.EventWarning:
		http.Error(w, `type must be "Normal" or "Warning"`, http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusCreated, s.store.RecordEvent(e))
}
