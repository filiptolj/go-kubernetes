package apiserver

import (
	"log"
	"net/http"
	"regexp"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// validNamespace matches the names Kubernetes allows for namespaces: lowercase
// letters, digits and dashes, starting and ending with a letter or digit, at
// most 63 characters. Namespace names end up in URLs and container names, so
// we keep them simple.
var validNamespace = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// handleListNamespaces returns all namespaces as JSON.
func (s *server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	writeList(w, r, s.store.ListNamespaces())
}

// handleCreateNamespace reads a namespace from the request body and stores it.
func (s *server) handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
	var ns api.Namespace
	if !decode(w, r, "namespace", &ns) {
		return
	}

	err := setKind(&ns.TypeMeta, "Namespace")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !validNamespace.MatchString(ns.Name) {
		http.Error(w, "namespace names use lowercase letters, digits and dashes, like \"team-a\"", http.StatusBadRequest)
		return
	}

	ns, err = s.store.CreateNamespace(ns)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("created namespace %q", ns.Name)
	writeJSON(w, http.StatusCreated, ns)
}

// handleDeleteNamespace removes a namespace and everything in it.
func (s *server) handleDeleteNamespace(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	err := s.store.DeleteNamespace(name)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("deleted namespace %q and everything in it", name)
	w.WriteHeader(http.StatusOK)
}
