package apiserver

import (
	"fmt"
	"log"
	"net/http"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// rules are what differs between kinds of object in serveResource.
type rules[T any] struct {
	// kind is the kind's name, such as "Job".
	kind string

	// typeMeta returns where an object keeps its apiVersion and kind.
	typeMeta func(*T) *api.TypeMeta

	// prepare fills in defaults and checks an object before it is stored. An
	// error becomes a 400 Bad Request answer.
	prepare func(*T) error

	// copyStatus copies the status part of src into dst. Kinds with a status
	// (written by a controller, not by users) set it: a normal update then
	// keeps the stored status, and only the /status route changes it.
	copyStatus func(dst *T, src T)
}

// serveResource registers the routes for one kind of namespaced object,
// such as jobs. plural is how the kind appears in URLs; the URLs start with
// the kind's API group, here /apis/batch/v1:
//
//	GET    /apis/batch/v1/jobs                                      in every namespace
//	GET    /apis/batch/v1/namespaces/{namespace}/jobs
//	POST   /apis/batch/v1/namespaces/{namespace}/jobs
//	GET    /apis/batch/v1/namespaces/{namespace}/jobs/{name}
//	PUT    /apis/batch/v1/namespaces/{namespace}/jobs/{name}
//	PUT    /apis/batch/v1/namespaces/{namespace}/jobs/{name}/status if the kind has a status
//	DELETE /apis/batch/v1/namespaces/{namespace}/jobs/{name}
func serveResource[T any](mux *http.ServeMux, st *store.Store, plural, label string, res *store.Resource[T], r rules[T]) {
	prefix := api.Prefix(plural)
	list := watchable(st, plural, func(w http.ResponseWriter, req *http.Request) {
		writeList(w, req, res.List(req.PathValue("namespace")))
	})
	mux.HandleFunc("GET "+prefix+"/"+plural, list)
	mux.HandleFunc("GET "+prefix+"/namespaces/{namespace}/"+plural, list)

	mux.HandleFunc("POST "+prefix+"/namespaces/{namespace}/"+plural, func(w http.ResponseWriter, req *http.Request) {
		var obj T
		if !decode(w, req, label, &obj) || !setNamespace(w, req, &res.Meta(&obj).Namespace) {
			return
		}
		m := res.Meta(&obj)
		if m.Name == "" {
			http.Error(w, label+" name is required", http.StatusBadRequest)
			return
		}
		if r.copyStatus != nil {
			var empty T
			r.copyStatus(&obj, empty) // a new object starts without a status
		}
		if !prepare(w, r, &obj) {
			return
		}

		obj, err := res.Create(obj)
		if err != nil {
			http.Error(w, err.Error(), statusForError(err))
			return
		}
		log.Printf("created %s %s", label, api.Key(m.Namespace, m.Name))
		writeJSON(w, http.StatusCreated, obj)
	})

	path := prefix + "/namespaces/{namespace}/" + plural + "/{name}"

	mux.HandleFunc("GET "+path, func(w http.ResponseWriter, req *http.Request) {
		namespace, name := req.PathValue("namespace"), req.PathValue("name")
		obj, ok := res.Get(namespace, name)
		if !ok {
			http.Error(w, fmt.Sprintf("%s %q not found in namespace %q", label, name, namespace), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, obj)
	})

	mux.HandleFunc("PUT "+path, func(w http.ResponseWriter, req *http.Request) {
		var obj T
		if !decode(w, req, label, &obj) || !setNamespace(w, req, &res.Meta(&obj).Namespace) {
			return
		}
		m := res.Meta(&obj)
		m.Name = req.PathValue("name") // the name in the URL wins

		if r.copyStatus != nil {
			old, ok := res.Get(m.Namespace, m.Name)
			if ok {
				r.copyStatus(&obj, old) // keep the status the controller wrote
			}
		}
		if !prepare(w, r, &obj) {
			return
		}

		obj, err := res.Update(obj)
		if err != nil {
			http.Error(w, err.Error(), statusForError(err))
			return
		}
		log.Printf("updated %s %s", label, api.Key(m.Namespace, m.Name))
		writeJSON(w, http.StatusOK, obj)
	})

	if r.copyStatus != nil {
		mux.HandleFunc("PUT "+path+"/status", func(w http.ResponseWriter, req *http.Request) {
			var update T
			if !decode(w, req, label, &update) {
				return
			}
			namespace, name := req.PathValue("namespace"), req.PathValue("name")
			obj, ok := res.Get(namespace, name)
			if !ok {
				http.Error(w, fmt.Sprintf("%s %q not found in namespace %q", label, name, namespace), http.StatusNotFound)
				return
			}

			r.copyStatus(&obj, update) // only the status changes

			// The update must be based on the current version. Copy its
			// resourceVersion, so a stale status update is refused.
			res.Meta(&obj).ResourceVersion = res.Meta(&update).ResourceVersion
			obj, err := res.Update(obj)
			if err != nil {
				http.Error(w, err.Error(), statusForError(err))
				return
			}
			writeJSON(w, http.StatusOK, obj)
		})
	}

	mux.HandleFunc("DELETE "+path, func(w http.ResponseWriter, req *http.Request) {
		namespace, name := req.PathValue("namespace"), req.PathValue("name")
		err := res.Delete(namespace, name)
		if err != nil {
			http.Error(w, err.Error(), statusForError(err))
			return
		}
		log.Printf("deleted %s %s", label, api.Key(namespace, name))
		w.WriteHeader(http.StatusOK)
	})
}

// prepare sets the object's apiVersion and kind, runs r.prepare if the kind
// has one, and answers 400 Bad Request if anything is wrong.
func prepare[T any](w http.ResponseWriter, r rules[T], obj *T) bool {
	err := setKind(r.typeMeta(obj), r.kind)
	if err == nil && r.prepare != nil {
		err = r.prepare(obj)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}
