package apiserver

import (
	"errors"
	"log"
	"net/http"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// handleListReplicaSets returns the ReplicaSets in the URL's namespace, or
// in every namespace for GET /api/replicasets.
func (s *server) handleListReplicaSets(w http.ResponseWriter, r *http.Request) {
	writeList(w, r, s.store.ListReplicaSets(r.PathValue("namespace")))
}

// handleCreateReplicaSet reads a ReplicaSet from the request body and stores it.
// handleUpdateReplicaSet replaces a ReplicaSet. The Deployment controller
// uses it to renumber an old ReplicaSet that becomes current again.
func (s *server) handleUpdateReplicaSet(w http.ResponseWriter, r *http.Request) {
	var rs api.ReplicaSet
	if !decode(w, r, "replicaset", &rs) || !setNamespace(w, r, &rs.Namespace) {
		return
	}
	rs.Name = r.PathValue("name") // the name in the URL wins

	err := checkReplicaSet(&rs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rs, err = s.store.UpdateReplicaSet(rs)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}
	log.Printf("updated replicaset %s", api.Key(rs.Namespace, rs.Name))
	writeJSON(w, http.StatusOK, rs)
}

// checkReplicaSet checks a ReplicaSet and fills in its defaults.
func checkReplicaSet(rs *api.ReplicaSet) error {
	switch {
	case rs.Name == "":
		return errors.New("replicaset name is required")
	case rs.Replicas < 0:
		return errors.New("replicas can't be negative")
	}
	err := setKind(&rs.TypeMeta, "ReplicaSet")
	if err != nil {
		return err
	}
	return prepareTemplate(&rs.Template)
}

func (s *server) handleCreateReplicaSet(w http.ResponseWriter, r *http.Request) {
	var rs api.ReplicaSet
	if !decode(w, r, "replicaset", &rs) || !setNamespace(w, r, &rs.Namespace) {
		return
	}

	err := checkReplicaSet(&rs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rs, err = s.store.CreateReplicaSet(rs)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("created replicaset %s (%d replicas)", api.Key(rs.Namespace, rs.Name), rs.Replicas)
	writeJSON(w, http.StatusCreated, rs)
}

// handleDeleteReplicaSet removes a ReplicaSet. The controller then removes its pods.
func (s *server) handleDeleteReplicaSet(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")

	err := s.store.DeleteReplicaSet(namespace, name)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("deleted replicaset %s", api.Key(namespace, name))
	w.WriteHeader(http.StatusOK)
}

// handleScaleReplicaSet changes how many replicas a ReplicaSet wants.
func (s *server) handleScaleReplicaSet(w http.ResponseWriter, r *http.Request) {
	var scale api.Scale
	if !decode(w, r, "scale", &scale) {
		return
	}

	if scale.Replicas < 0 {
		http.Error(w, "replicas can't be negative", http.StatusBadRequest)
		return
	}

	rs, err := s.store.ScaleReplicaSet(r.PathValue("namespace"), r.PathValue("name"), scale.Replicas)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("scaled replicaset %s to %d", api.Key(rs.Namespace, rs.Name), rs.Replicas)
	writeJSON(w, http.StatusOK, rs)
}

// validateDeployment fills in a Deployment's defaults and checks it.
func validateDeployment(d *api.Deployment) error {
	switch {
	case d.Name == "":
		return errors.New("deployment name is required")
	case d.Replicas < 0:
		return errors.New("replicas can't be negative")
	case len(d.Template.Labels) == 0:
		return errors.New("template needs labels, so the deployment's pods can be found")
	}
	err := setKind(&d.TypeMeta, "Deployment")
	if err != nil {
		return err
	}
	return prepareTemplate(&d.Template)
}

// handleListDeployments returns the Deployments in the URL's namespace, or
// in every namespace for GET /api/deployments.
func (s *server) handleListDeployments(w http.ResponseWriter, r *http.Request) {
	writeList(w, r, s.store.ListDeployments(r.PathValue("namespace")))
}

// handleCreateDeployment reads a Deployment from the request body and stores it.
func (s *server) handleCreateDeployment(w http.ResponseWriter, r *http.Request) {
	var d api.Deployment
	if !decode(w, r, "deployment", &d) || !setNamespace(w, r, &d.Namespace) {
		return
	}

	err := validateDeployment(&d)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	d, err = s.store.CreateDeployment(d)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("created deployment %s (%d replicas)", api.Key(d.Namespace, d.Name), d.Replicas)
	writeJSON(w, http.StatusCreated, d)
}

// handleUpdateDeployment replaces a Deployment. A new template starts a rolling update.
func (s *server) handleUpdateDeployment(w http.ResponseWriter, r *http.Request) {
	var d api.Deployment
	if !decode(w, r, "deployment", &d) || !setNamespace(w, r, &d.Namespace) {
		return
	}

	// The name in the URL wins, like for nodes.
	d.Name = r.PathValue("name")
	err := validateDeployment(&d)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.store.UpdateDeployment(d)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("updated deployment %s", api.Key(d.Namespace, d.Name))
	writeJSON(w, http.StatusOK, d)
}

// handleDeleteDeployment removes a Deployment. The controller then removes its ReplicaSets.
func (s *server) handleDeleteDeployment(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")

	err := s.store.DeleteDeployment(namespace, name)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("deleted deployment %s", api.Key(namespace, name))
	w.WriteHeader(http.StatusOK)
}

// handleScaleDeployment changes how many replicas a Deployment wants.
func (s *server) handleScaleDeployment(w http.ResponseWriter, r *http.Request) {
	var scale api.Scale
	if !decode(w, r, "scale", &scale) {
		return
	}

	if scale.Replicas < 0 {
		http.Error(w, "replicas can't be negative", http.StatusBadRequest)
		return
	}

	d, err := s.store.ScaleDeployment(r.PathValue("namespace"), r.PathValue("name"), scale.Replicas)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("scaled deployment %s to %d", api.Key(d.Namespace, d.Name), d.Replicas)
	writeJSON(w, http.StatusOK, d)
}

// handleListServices returns the Services in the URL's namespace, or in
// every namespace for GET /api/services.
func (s *server) handleListServices(w http.ResponseWriter, r *http.Request) {
	writeList(w, r, s.store.ListServices(r.PathValue("namespace")))
}

// handleCreateService reads a Service from the request body and stores it.
func (s *server) handleCreateService(w http.ResponseWriter, r *http.Request) {
	var svc api.Service
	if !decode(w, r, "service", &svc) || !setNamespace(w, r, &svc.Namespace) {
		return
	}

	if svc.Name == "" {
		http.Error(w, "service name is required", http.StatusBadRequest)
		return
	}
	err := setKind(&svc.TypeMeta, "Service")
	if err == nil {
		err = prepareService(&svc)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	svc, err = s.store.CreateService(svc)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("created service %s", api.Key(svc.Namespace, svc.Name))
	writeJSON(w, http.StatusCreated, svc)
}

// handleDeleteService removes a Service.
func (s *server) handleDeleteService(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")

	err := s.store.DeleteService(namespace, name)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("deleted service %s", api.Key(namespace, name))
	w.WriteHeader(http.StatusOK)
}
