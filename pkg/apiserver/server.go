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
// The URLs follow Kubernetes: the original kinds live under /api/v1, the
// others under /apis/<group>/<version>, such as /apis/apps/v1 for
// Deployments. Objects in a namespace are under .../namespaces/{namespace}/.
// Listing a kind without a namespace, such as GET /api/v1/pods, returns
// them from every namespace. Adding ?watch=true to a list turns it into a
// watch: see serveWatch.
func NewHandler(st *store.Store) http.Handler {
	s := &server{store: st}

	const core = "/api/v1"
	apps := api.Prefix("deployments")
	inNamespace := "/namespaces/{namespace}"

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)

	mux.HandleFunc("GET "+core+"/namespaces", watchable(st, "namespaces", s.handleListNamespaces))
	mux.HandleFunc("POST "+core+"/namespaces", s.handleCreateNamespace)
	mux.HandleFunc("DELETE "+core+"/namespaces/{name}", s.handleDeleteNamespace)

	mux.HandleFunc("GET "+core+"/pods", watchable(st, "pods", s.handleListPods))
	mux.HandleFunc("GET "+core+inNamespace+"/pods", watchable(st, "pods", s.handleListPods))
	mux.HandleFunc("POST "+core+inNamespace+"/pods", s.handleCreatePod)
	mux.HandleFunc("GET "+core+inNamespace+"/pods/{name}", s.handleGetPod)
	mux.HandleFunc("DELETE "+core+inNamespace+"/pods/{name}", s.handleDeletePod)
	mux.HandleFunc("POST "+core+inNamespace+"/pods/{name}/binding", s.handleBindPod)
	mux.HandleFunc("POST "+core+inNamespace+"/pods/{name}/status", s.handleSetPodStatus)

	mux.HandleFunc("GET "+core+"/nodes", watchable(st, "nodes", s.handleListNodes))
	mux.HandleFunc("POST "+core+"/nodes", s.handleCreateNode)
	mux.HandleFunc("PUT "+core+"/nodes/{name}", s.handlePutNode)
	mux.HandleFunc("POST "+core+"/nodes/{name}/status", s.handleSetNodeStatus)

	mux.HandleFunc("GET "+core+"/services", watchable(st, "services", s.handleListServices))
	mux.HandleFunc("GET "+core+inNamespace+"/services", watchable(st, "services", s.handleListServices))
	mux.HandleFunc("POST "+core+inNamespace+"/services", s.handleCreateService)
	mux.HandleFunc("DELETE "+core+inNamespace+"/services/{name}", s.handleDeleteService)

	mux.HandleFunc("GET "+core+"/events", s.handleListEvents)
	mux.HandleFunc("POST "+core+"/events", s.handleRecordEvent)

	mux.HandleFunc("GET "+apps+"/replicasets", watchable(st, "replicasets", s.handleListReplicaSets))
	mux.HandleFunc("GET "+apps+inNamespace+"/replicasets", watchable(st, "replicasets", s.handleListReplicaSets))
	mux.HandleFunc("POST "+apps+inNamespace+"/replicasets", s.handleCreateReplicaSet)
	mux.HandleFunc("DELETE "+apps+inNamespace+"/replicasets/{name}", s.handleDeleteReplicaSet)
	mux.HandleFunc("POST "+apps+inNamespace+"/replicasets/{name}/scale", s.handleScaleReplicaSet)

	mux.HandleFunc("GET "+apps+"/deployments", watchable(st, "deployments", s.handleListDeployments))
	mux.HandleFunc("GET "+apps+inNamespace+"/deployments", watchable(st, "deployments", s.handleListDeployments))
	mux.HandleFunc("POST "+apps+inNamespace+"/deployments", s.handleCreateDeployment)
	mux.HandleFunc("PUT "+apps+inNamespace+"/deployments/{name}", s.handleUpdateDeployment)
	mux.HandleFunc("DELETE "+apps+inNamespace+"/deployments/{name}", s.handleDeleteDeployment)
	mux.HandleFunc("POST "+apps+inNamespace+"/deployments/{name}/scale", s.handleScaleDeployment)

	// The newer kinds all use the same generic routes.
	serveResource(mux, st, "jobs", "job", st.Jobs, rules[api.Job]{
		kind:       "Job",
		typeMeta:   func(j *api.Job) *api.TypeMeta { return &j.TypeMeta },
		prepare:    prepareJob,
		copyStatus: func(dst *api.Job, src api.Job) { dst.Status = src.Status },
	})
	serveResource(mux, st, "cronjobs", "cronjob", st.CronJobs, rules[api.CronJob]{
		kind:       "CronJob",
		typeMeta:   func(c *api.CronJob) *api.TypeMeta { return &c.TypeMeta },
		prepare:    prepareCronJob,
		copyStatus: func(dst *api.CronJob, src api.CronJob) { dst.Status = src.Status },
	})
	serveResource(mux, st, "daemonsets", "daemonset", st.DaemonSets, rules[api.DaemonSet]{
		kind:     "DaemonSet",
		typeMeta: func(d *api.DaemonSet) *api.TypeMeta { return &d.TypeMeta },
		prepare:  prepareDaemonSet,
	})
	serveResource(mux, st, "statefulsets", "statefulset", st.StatefulSets, rules[api.StatefulSet]{
		kind:     "StatefulSet",
		typeMeta: func(ss *api.StatefulSet) *api.TypeMeta { return &ss.TypeMeta },
		prepare:  prepareStatefulSet,
	})
	serveResource(mux, st, "configmaps", "configmap", st.ConfigMaps, rules[api.ConfigMap]{
		kind:     "ConfigMap",
		typeMeta: func(c *api.ConfigMap) *api.TypeMeta { return &c.TypeMeta },
		prepare:  prepareConfigMap,
	})
	serveResource(mux, st, "ingresses", "ingress", st.Ingresses, rules[api.Ingress]{
		kind:     "Ingress",
		typeMeta: func(i *api.Ingress) *api.TypeMeta { return &i.TypeMeta },
		prepare:  prepareIngress,
	})
	serveResource(mux, st, "secrets", "secret", st.Secrets, rules[api.Secret]{
		kind:     "Secret",
		typeMeta: func(sec *api.Secret) *api.TypeMeta { return &sec.TypeMeta },
		prepare:  prepareSecret,
	})
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
	writeList(w, r, s.store.ListNodes())
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
	err := setKind(&node.TypeMeta, "Node")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	node, err = s.store.CreateNode(node)
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
	err := setKind(&node.TypeMeta, "Node")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	node, err = s.store.PutNode(node)
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
