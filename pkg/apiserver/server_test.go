package apiserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// TestAPI sends a series of requests to one server, in order, and checks the
// status code of each answer. Later steps rely on the earlier ones.
func TestAPI(t *testing.T) {
	handler := NewHandler(store.New())

	steps := []struct {
		method string
		path   string
		body   string
		want   int
	}{
		{"GET", "/healthz", "", http.StatusOK},
		{"GET", "/api/pods", "", http.StatusOK},
		{"GET", "/api/namespaces", "", http.StatusOK},

		// Creating pods
		{"POST", "/api/namespaces/default/pods", `{"name":"nginx","containers":[{"name":"c","image":"nginx"}]}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/pods", `{"name":"nginx","containers":[{"name":"c","image":"nginx"}]}`, http.StatusConflict},
		{"POST", "/api/namespaces/default/pods", `{"name":`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/pods", `{}`, http.StatusBadRequest},
		{"GET", "/api/namespaces/default/pods/nginx", "", http.StatusOK},
		{"GET", "/api/namespaces/default/pods/banana", "", http.StatusNotFound},

		// Binding needs an existing pod and node
		{"POST", "/api/namespaces/default/pods/nginx/binding", `{"nodeName":"node-1"}`, http.StatusNotFound},
		{"PUT", "/api/nodes/node-1", `{"ready":true}`, http.StatusOK},
		{"POST", "/api/namespaces/default/pods/nginx/binding", `{"nodeName":"node-1"}`, http.StatusOK},
		{"POST", "/api/namespaces/default/pods/nginx/binding", `{"nodeName":"node-1"}`, http.StatusConflict},
		{"POST", "/api/namespaces/default/pods/nginx/binding", `{}`, http.StatusBadRequest},

		// Phases
		{"POST", "/api/namespaces/default/pods/nginx/status", `{"phase":"Running"}`, http.StatusOK},
		{"POST", "/api/namespaces/default/pods/nginx/status", `{"phase":"Banana"}`, http.StatusBadRequest},

		// Namespaces
		{"POST", "/api/namespaces", `{"name":"dev"}`, http.StatusCreated},
		{"POST", "/api/namespaces", `{"name":"dev"}`, http.StatusConflict},
		{"POST", "/api/namespaces", `{"name":"Not_Valid"}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/nope/pods", `{"name":"x","containers":[{"name":"c","image":"nginx"}]}`, http.StatusNotFound},
		{"POST", "/api/namespaces/dev/pods", `{"name":"nginx","containers":[{"name":"c","image":"nginx"}]}`, http.StatusCreated}, // same name, other namespace: fine
		{"POST", "/api/namespaces/dev/pods", `{"name":"x","namespace":"default","containers":[{"name":"c","image":"nginx"}]}`, http.StatusBadRequest},
		{"GET", "/api/namespaces/dev/pods/nginx", "", http.StatusOK},
		{"DELETE", "/api/namespaces/default", "", http.StatusConflict},
		{"DELETE", "/api/namespaces/dev", "", http.StatusOK},
		{"GET", "/api/namespaces/dev/pods/nginx", "", http.StatusNotFound},
		{"GET", "/api/namespaces/default/pods/nginx", "", http.StatusOK}, // default's nginx is untouched

		// Pod validation
		{"POST", "/api/namespaces/default/pods", `{"name":"empty"}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/pods", `{"name":"p","restartPolicy":"Sometimes","containers":[{"name":"c","image":"nginx"}]}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/pods", `{"name":"p","containers":[{"name":"c","livenessProbe":{}}]}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/pods", `{"name":"p","containers":[{"name":"c","volumeMounts":[{"name":"nope","mountPath":"/x"}]}]}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/pods", `{"name":"p","volumes":[{"name":"v"}],"containers":[{"name":"c","image":"nginx"}]}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/pods", `{"name":"p","containers":[{"name":"c","env":[{"name":"X","valueFrom":{}}]}]}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/pods", `{"name":"ok","volumes":[{"name":"v","emptyDir":{}}],"containers":[{"name":"c","volumeMounts":[{"name":"v","mountPath":"/data"}]}]}`, http.StatusCreated},

		// Jobs and CronJobs
		{"POST", "/api/namespaces/default/jobs", `{"name":"pi","template":"containers":[{"name":"c","image":"nginx"}]}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/jobs", `{"name":"pi","template":{"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/jobs", `{"name":"bad","template":{"restartPolicy":"Always","containers":[{"name":"c","image":"nginx"}]}}`, http.StatusBadRequest},
		{"GET", "/api/namespaces/default/jobs/pi", "", http.StatusOK},
		{"PUT", "/api/namespaces/default/jobs/pi/status", `{"status":{"succeeded":1}}`, http.StatusOK},
		{"PUT", "/api/namespaces/default/jobs/nope/status", `{"status":{}}`, http.StatusNotFound},
		{"POST", "/api/namespaces/default/cronjobs", `{"name":"c","schedule":"*/5 * * * *","jobTemplate":{"template":{"containers":[{"name":"c","image":"nginx"}]}}}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/cronjobs", `{"name":"c2","schedule":"every day","jobTemplate":{"template":{"containers":[{"name":"c","image":"nginx"}]}}}`, http.StatusBadRequest},
		{"GET", "/api/cronjobs", "", http.StatusOK},

		// DaemonSets, StatefulSets, ConfigMaps, Secrets
		{"POST", "/api/namespaces/default/daemonsets", `{"name":"agent","template":{"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/daemonsets", `{"name":"agent2","template":{"restartPolicy":"Never","containers":[{"name":"c","image":"nginx"}]}}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/statefulsets", `{"name":"db","replicas":3,"template":{"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/statefulsets", `{"name":"db2","replicas":-1,"template":{"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/configmaps", `{"name":"settings","data":{"mode":"fast"}}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/configmaps", `{"name":"bad","data":{"../x":"y"}}`, http.StatusBadRequest},
		{"PUT", "/api/namespaces/default/configmaps/settings", `{"data":{"mode":"slow"}}`, http.StatusOK},
		{"POST", "/api/namespaces/default/secrets", `{"name":"db","data":{"password":"hunter2"}}`, http.StatusCreated},
		{"DELETE", "/api/namespaces/default/secrets/db", "", http.StatusOK},
		{"DELETE", "/api/namespaces/default/secrets/db", "", http.StatusNotFound},

		// ReplicaSets
		{"POST", "/api/namespaces/default/replicasets", `{"name":"web","replicas":2,"template":{"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/replicasets", `{"name":"bad","replicas":-1,"template":{"containers":[{"name":"c"}]}}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/replicasets", `{"name":"empty","replicas":1}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/replicasets/web/scale", `{"replicas":5}`, http.StatusOK},
		{"POST", "/api/namespaces/default/replicasets/nope/scale", `{"replicas":5}`, http.StatusNotFound},
		{"DELETE", "/api/namespaces/default/replicasets/web", "", http.StatusOK},

		// Deployments
		{"POST", "/api/namespaces/default/deployments", `{"name":"app","replicas":2,"template":{"labels":{"app":"x"},"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/deployments", `{"name":"app","replicas":2,"template":{"labels":{"app":"x"},"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusConflict},
		{"POST", "/api/namespaces/default/deployments", `{"name":"nolabels","replicas":1,"template":{"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusBadRequest},
		{"PUT", "/api/namespaces/default/deployments/app", `{"replicas":2,"template":{"labels":{"app":"x"},"containers":[{"name":"c","image":"nginx:2"}]}}`, http.StatusOK},
		{"PUT", "/api/namespaces/default/deployments/nope", `{"replicas":2,"template":{"labels":{"app":"x"},"containers":[{"name":"c","image":"nginx"}]}}`, http.StatusNotFound},
		{"POST", "/api/namespaces/default/deployments/app/scale", `{"replicas":4}`, http.StatusOK},
		{"DELETE", "/api/namespaces/default/deployments/app", "", http.StatusOK},

		// Services
		{"POST", "/api/namespaces/default/services", `{"name":"web","port":8081,"selector":{"app":"web"}}`, http.StatusCreated},
		{"POST", "/api/namespaces/default/services", `{"name":"other","port":8081,"selector":{"app":"web"}}`, http.StatusConflict},
		{"POST", "/api/namespaces/default/services", `{"name":"bad","port":99999,"selector":{"app":"web"}}`, http.StatusBadRequest},
		{"POST", "/api/namespaces/default/services", `{"name":"all","port":8082}`, http.StatusBadRequest},
		{"DELETE", "/api/namespaces/default/services/web", "", http.StatusOK},

		// Events
		{"POST", "/api/events", `{"kind":"Pod","namespace":"default","name":"nginx","type":"Normal","reason":"Scheduled","message":"hi"}`, http.StatusCreated},
		{"POST", "/api/events", `{"kind":"Pod","name":"nginx","type":"Fine","reason":"Scheduled"}`, http.StatusBadRequest},
		{"POST", "/api/events", `{"type":"Normal","reason":"Scheduled"}`, http.StatusBadRequest},
		{"GET", "/api/events?namespace=default&kind=Pod&name=nginx", "", http.StatusOK},

		// Deleting
		{"DELETE", "/api/namespaces/default/pods/nginx", "", http.StatusOK},
		{"DELETE", "/api/namespaces/default/pods/nginx", "", http.StatusNotFound},

		// Wrong method on a known path
		{"PATCH", "/api/pods", "", http.StatusMethodNotAllowed},
	}

	for _, step := range steps {
		req := httptest.NewRequest(step.method, step.path, strings.NewReader(step.body))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != step.want {
			t.Errorf("%s %s %s: got status %d, want %d (body: %s)",
				step.method, step.path, step.body, rec.Code, step.want, strings.TrimSpace(rec.Body.String()))
		}
	}
}
