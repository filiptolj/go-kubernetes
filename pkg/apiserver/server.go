// Package apiserver serves the cluster's API over HTTP. It is the only part
// of the system that reads and writes the store; every other component talks
// to it.
package apiserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// server holds everything the HTTP handlers need.
type server struct {
	store *store.Store
}

// NewHandler returns an http.Handler that serves the API, backed by st.
//
// Objects that live in a namespace are under /api/namespaces/{namespace}/,
// like in real Kubernetes. Listing them without a namespace, such as
// GET /api/pods, returns them from every namespace.
func NewHandler(st *store.Store) http.Handler {
	s := &server{store: st}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)

	mux.HandleFunc("GET /api/namespaces", s.handleListNamespaces)
	mux.HandleFunc("POST /api/namespaces", s.handleCreateNamespace)
	mux.HandleFunc("DELETE /api/namespaces/{name}", s.handleDeleteNamespace)

	mux.HandleFunc("GET /api/pods", s.handleListPods)
	mux.HandleFunc("GET /api/watch/pods", s.handleWatchPods)
	mux.HandleFunc("GET /api/namespaces/{namespace}/pods", s.handleListPods)
	mux.HandleFunc("POST /api/namespaces/{namespace}/pods", s.handleCreatePod)
	mux.HandleFunc("GET /api/namespaces/{namespace}/pods/{name}", s.handleGetPod)
	mux.HandleFunc("DELETE /api/namespaces/{namespace}/pods/{name}", s.handleDeletePod)
	mux.HandleFunc("POST /api/namespaces/{namespace}/pods/{name}/binding", s.handleBindPod)
	mux.HandleFunc("POST /api/namespaces/{namespace}/pods/{name}/status", s.handleSetPodStatus)

	mux.HandleFunc("GET /api/nodes", s.handleListNodes)
	mux.HandleFunc("POST /api/nodes", s.handleCreateNode)
	mux.HandleFunc("PUT /api/nodes/{name}", s.handlePutNode)
	mux.HandleFunc("POST /api/nodes/{name}/status", s.handleSetNodeStatus)

	mux.HandleFunc("GET /api/replicasets", s.handleListReplicaSets)
	mux.HandleFunc("GET /api/namespaces/{namespace}/replicasets", s.handleListReplicaSets)
	mux.HandleFunc("POST /api/namespaces/{namespace}/replicasets", s.handleCreateReplicaSet)
	mux.HandleFunc("DELETE /api/namespaces/{namespace}/replicasets/{name}", s.handleDeleteReplicaSet)
	mux.HandleFunc("POST /api/namespaces/{namespace}/replicasets/{name}/scale", s.handleScaleReplicaSet)

	mux.HandleFunc("GET /api/deployments", s.handleListDeployments)
	mux.HandleFunc("GET /api/namespaces/{namespace}/deployments", s.handleListDeployments)
	mux.HandleFunc("POST /api/namespaces/{namespace}/deployments", s.handleCreateDeployment)
	mux.HandleFunc("PUT /api/namespaces/{namespace}/deployments/{name}", s.handleUpdateDeployment)
	mux.HandleFunc("DELETE /api/namespaces/{namespace}/deployments/{name}", s.handleDeleteDeployment)
	mux.HandleFunc("POST /api/namespaces/{namespace}/deployments/{name}/scale", s.handleScaleDeployment)

	mux.HandleFunc("GET /api/services", s.handleListServices)
	mux.HandleFunc("GET /api/namespaces/{namespace}/services", s.handleListServices)
	mux.HandleFunc("POST /api/namespaces/{namespace}/services", s.handleCreateService)
	mux.HandleFunc("DELETE /api/namespaces/{namespace}/services/{name}", s.handleDeleteService)

	mux.HandleFunc("GET /api/events", s.handleListEvents)
	mux.HandleFunc("POST /api/events", s.handleRecordEvent)

	// The newer kinds all use the same generic routes.
	serveResource(mux, "jobs", "job", st.Jobs, rules[api.Job]{
		prepare:    prepareJob,
		copyStatus: func(dst *api.Job, src api.Job) { dst.Status = src.Status },
	})
	serveResource(mux, "cronjobs", "cronjob", st.CronJobs, rules[api.CronJob]{
		prepare:    prepareCronJob,
		copyStatus: func(dst *api.CronJob, src api.CronJob) { dst.Status = src.Status },
	})
	serveResource(mux, "daemonsets", "daemonset", st.DaemonSets, rules[api.DaemonSet]{prepare: prepareDaemonSet})
	serveResource(mux, "statefulsets", "statefulset", st.StatefulSets, rules[api.StatefulSet]{prepare: prepareStatefulSet})
	serveResource(mux, "configmaps", "configmap", st.ConfigMaps, rules[api.ConfigMap]{prepare: prepareConfigMap})
	serveResource(mux, "secrets", "secret", st.Secrets, rules[api.Secret]{prepare: prepareSecret})
	return mux
}

// handleHealthz tells callers the server is alive.
func handleHealthz(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok")
}

// decode reads the request body as JSON into v. If that fails, it answers
// 400 Bad Request and returns false.
func decode(w http.ResponseWriter, r *http.Request, what string, v any) bool {
	err := json.NewDecoder(r.Body).Decode(v)
	if err != nil {
		http.Error(w, "invalid "+what+" JSON: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

// setNamespace puts the URL's namespace into an object's namespace field. It
// takes a pointer to the field so it can change it. An object that names a
// different namespace than the URL is refused.
func setNamespace(w http.ResponseWriter, r *http.Request, field *string) bool {
	namespace := r.PathValue("namespace")
	if *field != "" && *field != namespace {
		http.Error(w, fmt.Sprintf("the object says namespace %q, but the URL is for namespace %q", *field, namespace),
			http.StatusBadRequest)
		return false
	}
	*field = namespace
	return true
}

// statusForError picks the HTTP status code that matches a store error.
func statusForError(err error) int {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, store.ErrConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// writeJSON sends v as a JSON answer with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(v)
	if err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

// handleListNodes returns all nodes as JSON.
func (s *server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.ListNodes())
}

// handleCreateNode reads a node from the request body and stores it.
func (s *server) handleCreateNode(w http.ResponseWriter, r *http.Request) {
	var node api.Node
	if !decode(w, r, "node", &node) {
		return
	}

	if node.Name == "" {
		http.Error(w, "node name is required", http.StatusBadRequest)
		return
	}

	err := s.store.CreateNode(node)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("created node %q", node.Name)
	writeJSON(w, http.StatusCreated, node)
}

// handlePutNode creates or replaces a node. Kubelets call it every few seconds
// as a heartbeat.
func (s *server) handlePutNode(w http.ResponseWriter, r *http.Request) {
	var node api.Node
	if !decode(w, r, "node", &node) {
		return
	}

	// The name in the URL wins, so a kubelet can't update the wrong node by accident.
	node.Name = r.PathValue("name")
	node, err := s.store.PutNode(node)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	writeJSON(w, http.StatusOK, node)
}

// handleSetNodeStatus marks a node Ready or NotReady. The node controller calls it.
func (s *server) handleSetNodeStatus(w http.ResponseWriter, r *http.Request) {
	var status api.NodeStatus
	if !decode(w, r, "status", &status) {
		return
	}

	node, err := s.store.SetNodeReady(r.PathValue("name"), status.Ready)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("node %q ready: %t", node.Name, node.Ready)
	writeJSON(w, http.StatusOK, node)
}
