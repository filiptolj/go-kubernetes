package apiserver

import (
	"encoding/json"
	"log"
	"net/http"
	"reflect"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// writeList sends a list of objects, only those that match the request's
// ?labelSelector= if it has one, such as ?labelSelector=app%3Dweb.
func writeList[T any](w http.ResponseWriter, r *http.Request, list []T) {
	sel, err := api.ParseSelector(r.URL.Query().Get("labelSelector"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	selected := []T{} // not nil: an empty list is sent as [], not null
	for i := range list {
		if sel.Matches(api.MetaOf(&list[i]).Labels) {
			selected = append(selected, list[i])
		}
	}
	writeJSON(w, http.StatusOK, selected)
}

// watchable returns a handler that serves list for a normal GET, and a
// watch of the same objects for GET ...?watch=true, as Kubernetes does.
func watchable(st *store.Store, plural string, list http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" {
			serveWatch(st, plural, w, r)
			return
		}
		list(w, r)
	}
}

// serveWatch keeps the connection open and sends one line of JSON, like
// {"type":"MODIFIED","object":{...}}, every time an object of kind plural
// changes, until the client disconnects. A list URL with a namespace, such
// as /api/v1/namespaces/dev/pods?watch=true, only sees changes in it.
//
// With ?labelSelector=, it only sends changes to objects whose labels match
// (after the change).
//
// A watch only sends changes from now on. To see everything, clients start a
// watch first and list second, so nothing can slip through the gap.
func serveWatch(st *store.Store, plural string, w http.ResponseWriter, r *http.Request) {
	sel, err := api.ParseSelector(r.URL.Query().Get("labelSelector"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	events, stop := st.Watch(plural, r.PathValue("namespace"))
	defer stop()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Flush sends what has been written so far right away, instead of
	// waiting for the buffer to fill up. Without it, the client wouldn't
	// know the watch had started.
	rc := http.NewResponseController(w)
	rc.Flush()

	enc := json.NewEncoder(w)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				log.Printf("watch of %s dropped: client too slow", plural)
				return
			}
			if !sel.Matches(labelsOf(event.Object)) {
				continue
			}
			err := enc.Encode(event)
			if err != nil {
				return
			}
			rc.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// labelsOf returns the labels of an object of any kind, held in an any.
// api.MetaOf needs a pointer to the object, and an any holding a value can't
// give one, so reflect makes a copy we can point to.
func labelsOf(obj any) api.Labels {
	v := reflect.New(reflect.TypeOf(obj))
	v.Elem().Set(reflect.ValueOf(obj))
	return api.MetaOf(v.Interface()).Labels
}
