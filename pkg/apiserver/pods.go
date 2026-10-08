package apiserver

import (
	"fmt"
	"log"
	"net/http"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// handleListPods returns the pods in the URL's namespace, or in every
// namespace for GET /api/pods.
func (s *server) handleListPods(w http.ResponseWriter, r *http.Request) {
	writeList(w, r, s.store.ListPods(r.PathValue("namespace")))
}

// handleCreatePod reads a pod from the request body and stores it.
func (s *server) handleCreatePod(w http.ResponseWriter, r *http.Request) {
	var pod api.Pod
	if !decode(w, r, "pod", &pod) || !setNamespace(w, r, &pod.Namespace) {
		return
	}

	if pod.Name == "" {
		http.Error(w, "pod name is required", http.StatusBadRequest)
		return
	}

	err := setKind(&pod.TypeMeta, "Pod")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if pod.RestartPolicy == "" {
		pod.RestartPolicy = api.RestartAlways
	}
	err = validatePodSpec(&pod.PodSpec, api.RestartAlways, api.RestartOnFailure, api.RestartNever)
	if err != nil {
		http.Error(w, "pod: "+err.Error(), http.StatusBadRequest)
		return
	}

	pod.Phase = api.PodPending

	pod, err = s.store.CreatePod(pod)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("created pod %s", api.Key(pod.Namespace, pod.Name))
	writeJSON(w, http.StatusCreated, pod)
}

// handleGetPod returns a single pod.
func (s *server) handleGetPod(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")

	pod, ok := s.store.GetPod(namespace, name)
	if !ok {
		http.Error(w, fmt.Sprintf("pod %q not found in namespace %q", name, namespace), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, pod)
}

// handleDeletePod removes a pod. Its kubelet sees the DELETED event and stops it.
func (s *server) handleDeletePod(w http.ResponseWriter, r *http.Request) {
	pod, err := s.store.DeletePod(r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("deleted pod %s", api.Key(pod.Namespace, pod.Name))
	writeJSON(w, http.StatusOK, pod)
}

// handleBindPod assigns a pod to the node named in the request body.
func (s *server) handleBindPod(w http.ResponseWriter, r *http.Request) {
	var binding api.Binding
	if !decode(w, r, "binding", &binding) {
		return
	}

	if binding.NodeName == "" {
		http.Error(w, "nodeName is required", http.StatusBadRequest)
		return
	}

	pod, err := s.store.BindPod(r.PathValue("namespace"), r.PathValue("name"), binding.NodeName)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("bound pod %s to node %q", api.Key(pod.Namespace, pod.Name), pod.NodeName)
	writeJSON(w, http.StatusOK, pod)
}

// handleSetPodStatus changes a pod's phase. The kubelet calls this.
func (s *server) handleSetPodStatus(w http.ResponseWriter, r *http.Request) {
	var status api.PodStatus
	if !decode(w, r, "status", &status) {
		return
	}

	switch status.Phase {
	case api.PodPending, api.PodRunning, api.PodSucceeded, api.PodFailed:
		// a phase we know, carry on
	default:
		http.Error(w, fmt.Sprintf("invalid phase %q", status.Phase), http.StatusBadRequest)
		return
	}

	pod, err := s.store.SetPodStatus(r.PathValue("namespace"), r.PathValue("name"), status)
	if err != nil {
		http.Error(w, err.Error(), statusForError(err))
		return
	}

	log.Printf("pod %s is now %s", api.Key(pod.Namespace, pod.Name), pod.Phase)
	writeJSON(w, http.StatusOK, pod)
}
