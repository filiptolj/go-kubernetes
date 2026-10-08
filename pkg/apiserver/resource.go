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
	// prepare fills in defaults and checks an object before it is stored. An
	// error becomes a 400 Bad Request answer.
	prepare func(*T) error

	// copyStatus copies the status part of src into dst. Kinds with a status
	// (written by a controller, not by users) set it: a normal update then
	// keeps the stored status, and only the /status route changes it.
	copyStatus func(dst *T, src T)
}

// serveResource registers the routes for one kind of namespaced object,
// such as jobs. plural is how the kind appears in URLs:
//
//	GET    /api/jobs                                      in every namespace
//	GET    /api/namespaces/{namespace}/jobs
//	POST   /api/namespaces/{namespace}/jobs
//	GET    /api/namespaces/{namespace}/jobs/{name}
//	PUT    /api/namespaces/{namespace}/jobs/{name}
//	PUT    /api/namespaces/{namespace}/jobs/{name}/status if the kind has a status
//	DELETE /api/namespaces/{namespace}/jobs/{name}
func serveResource[T any](mux *http.ServeMux, plural, label string, res *store.Resource[T], r rules[T]) {
	list := func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, http.StatusOK, res.List(req.PathValue("namespace")))
	}
	mux.HandleFunc("GET /api/"+plural, list)
	mux.HandleFunc("GET /api/namespaces/{namespace}/"+plural, list)

	mux.HandleFunc("POST /api/namespaces/{namespace}/"+plural, func(w http.ResponseWriter, req *http.Request) {
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

		err := res.Create(obj)
		if err != nil {
			http.Error(w, err.Error(), statusForError(err))
			return
		}
		log.Printf("created %s %s", label, api.Key(m.Namespace, m.Name))
		writeJSON(w, http.StatusCreated, obj)
	})

	path := "/api/namespaces/{namespace}/" + plural + "/{name}"

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

		err := res.Update(obj)
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
			err := res.Update(obj)
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

// prepare runs r.prepare, if the kind has one, and answers 400 Bad Request
// if it fails.
func prepare[T any](w http.ResponseWriter, r rules[T], obj *T) bool {
	if r.prepare == nil {
		return true
	}
	err := r.prepare(obj)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}
