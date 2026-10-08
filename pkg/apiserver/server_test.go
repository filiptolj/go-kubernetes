package apiserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// Pieces of request bodies the steps below share.
const (
	nginx    = `"containers":[{"name":"c","image":"nginx"}]`
	template = `"template":{"spec":{` + nginx + `}}`
)

// pod returns the body of a pod with the given name and spec.
func pod(name, spec string) string {
	return `{"metadata":{"name":"` + name + `"},"spec":{` + spec + `}}`
}

// TestAPI sends a series of requests to one server, in order, and checks the
// status code of each answer. Later steps rely on the earlier ones.
func TestAPI(t *testing.T) {
	handler := NewHandler(store.New())

	const (
		core  = "/api/v1"
		dflt  = core + "/namespaces/default"
		apps  = "/apis/apps/v1/namespaces/default"
		batch = "/apis/batch/v1/namespaces/default"
	)

	steps := []struct {
		method string
		path   string
		body   string
		want   int
	}{
		{"GET", "/healthz", "", http.StatusOK},
		{"GET", core + "/pods", "", http.StatusOK},
		{"GET", core + "/namespaces", "", http.StatusOK},

		// Creating pods
		{"POST", dflt + "/pods", pod("nginx", nginx), http.StatusCreated},
		{"POST", dflt + "/pods", pod("nginx", nginx), http.StatusConflict},
		{"POST", dflt + "/pods", `{"metadata":`, http.StatusBadRequest},
		{"POST", dflt + "/pods", `{}`, http.StatusBadRequest},
		{"POST", dflt + "/pods", `{"kind":"Service",` + pod("wrong-kind", nginx)[1:], http.StatusBadRequest},
		{"POST", dflt + "/pods", `{"apiVersion":"apps/v1",` + pod("wrong-version", nginx)[1:], http.StatusBadRequest},
		{"POST", dflt + "/pods", `{"apiVersion":"v1","kind":"Pod",` + pod("right-kind", nginx)[1:], http.StatusCreated},
		{"GET", dflt + "/pods/nginx", "", http.StatusOK},
		{"GET", dflt + "/pods/banana", "", http.StatusNotFound},

		// Binding needs an existing pod and node
		{"POST", dflt + "/pods/nginx/binding", `{"nodeName":"node-1"}`, http.StatusNotFound},
		{"PUT", core + "/nodes/node-1", `{"status":{"ready":true}}`, http.StatusOK},
		{"POST", dflt + "/pods/nginx/binding", `{"nodeName":"node-1"}`, http.StatusOK},
		{"POST", dflt + "/pods/nginx/binding", `{"nodeName":"node-1"}`, http.StatusConflict},
		{"POST", dflt + "/pods/nginx/binding", `{}`, http.StatusBadRequest},

		// Phases
		{"POST", dflt + "/pods/nginx/status", `{"phase":"Running"}`, http.StatusOK},
		{"POST", dflt + "/pods/nginx/status", `{"phase":"Banana"}`, http.StatusBadRequest},

		// Namespaces
		{"POST", core + "/namespaces", `{"metadata":{"name":"dev"}}`, http.StatusCreated},
		{"POST", core + "/namespaces", `{"metadata":{"name":"dev"}}`, http.StatusConflict},
		{"POST", core + "/namespaces", `{"metadata":{"name":"Not_Valid"}}`, http.StatusBadRequest},
		{"POST", core + "/namespaces/nope/pods", pod("x", nginx), http.StatusNotFound},
		{"POST", core + "/namespaces/dev/pods", pod("nginx", nginx), http.StatusCreated}, // same name, other namespace: fine
		{"POST", core + "/namespaces/dev/pods", `{"metadata":{"name":"x","namespace":"default"},"spec":{` + nginx + `}}`, http.StatusBadRequest},
		{"GET", core + "/namespaces/dev/pods/nginx", "", http.StatusOK},
		{"DELETE", core + "/namespaces/default", "", http.StatusConflict},
		{"DELETE", core + "/namespaces/dev", "", http.StatusOK},
		{"GET", core + "/namespaces/dev/pods/nginx", "", http.StatusNotFound},
		{"GET", dflt + "/pods/nginx", "", http.StatusOK}, // default's nginx is untouched

		// Pod validation
		{"POST", dflt + "/pods", pod("empty", ""), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"restartPolicy":"Sometimes",`+nginx), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"containers":[{"name":"c","livenessProbe":{}}]`), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"containers":[{"name":"c","volumeMounts":[{"name":"nope","mountPath":"/x"}]}]`), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"volumes":[{"name":"v"}],`+nginx), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"containers":[{"name":"c","env":[{"name":"X","valueFrom":{}}]}]`), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"containers":[{"name":"c","ports":[{"containerPort":99999}]}]`), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"containers":[{"name":"c","resources":{"limits":{"memory":"lots"}}}]`), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"containers":[{"name":"c","resources":{"requests":{"gpu":"1"}}}]`), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("p", `"containers":[{"name":"c","resources":{"requests":{"cpu":"2"},"limits":{"cpu":"500m"}}}]`), http.StatusBadRequest},
		{"POST", dflt + "/pods", pod("sized", `"containers":[{"name":"c","resources":{"requests":{"cpu":"250m"},"limits":{"cpu":"1","memory":"64Mi"}}}]`), http.StatusCreated},
		{"POST", dflt + "/pods", pod("ok", `"volumes":[{"name":"v","emptyDir":{}}],"containers":[{"name":"c","volumeMounts":[{"name":"v","mountPath":"/data"}]}]`), http.StatusCreated},

		// Jobs and CronJobs
		{"POST", batch + "/jobs", `{"metadata":{"name":"pi"},"spec":{"template":"spec":{}}}`, http.StatusBadRequest},
		{"POST", batch + "/jobs", `{"metadata":{"name":"pi"},"spec":{` + template + `}}`, http.StatusCreated},
		{"POST", batch + "/jobs", `{"metadata":{"name":"bad"},"spec":{"template":{"spec":{"restartPolicy":"Always",` + nginx + `}}}}`, http.StatusBadRequest},
		{"GET", batch + "/jobs/pi", "", http.StatusOK},
		{"PUT", batch + "/jobs/pi/status", `{"status":{"succeeded":1}}`, http.StatusOK},
		{"PUT", batch + "/jobs/nope/status", `{"status":{}}`, http.StatusNotFound},
		{"POST", batch + "/cronjobs", `{"metadata":{"name":"c"},"spec":{"schedule":"*/5 * * * *","jobTemplate":{"spec":{` + template + `}}}}`, http.StatusCreated},
		{"POST", batch + "/cronjobs", `{"metadata":{"name":"c2"},"spec":{"schedule":"every day","jobTemplate":{"spec":{` + template + `}}}}`, http.StatusBadRequest},
		{"GET", "/apis/batch/v1/cronjobs", "", http.StatusOK},

		// DaemonSets, StatefulSets, ConfigMaps, Secrets
		{"POST", apps + "/daemonsets", `{"metadata":{"name":"agent"},"spec":{` + template + `}}`, http.StatusCreated},
		{"POST", apps + "/daemonsets", `{"metadata":{"name":"agent2"},"spec":{"template":{"spec":{"restartPolicy":"Never",` + nginx + `}}}}`, http.StatusBadRequest},
		{"POST", apps + "/statefulsets", `{"metadata":{"name":"db"},"spec":{"replicas":3,` + template + `}}`, http.StatusCreated},
		{"POST", apps + "/statefulsets", `{"metadata":{"name":"db2"},"spec":{"replicas":-1,` + template + `}}`, http.StatusBadRequest},
		{"POST", dflt + "/configmaps", `{"metadata":{"name":"settings"},"data":{"mode":"fast"}}`, http.StatusCreated},
		{"POST", dflt + "/configmaps", `{"metadata":{"name":"bad"},"data":{"../x":"y"}}`, http.StatusBadRequest},
		{"PUT", dflt + "/configmaps/settings", `{"data":{"mode":"slow"}}`, http.StatusOK},
		{"PUT", dflt + "/configmaps/settings", `{"metadata":{"resourceVersion":"1"},"data":{"mode":"stale"}}`, http.StatusConflict},
		{"POST", dflt + "/secrets", `{"metadata":{"name":"db"},"data":{"password":"aHVudGVyMg=="}}`, http.StatusCreated},
		{"POST", dflt + "/secrets", `{"metadata":{"name":"db2"},"data":{"password":"not base64!"}}`, http.StatusBadRequest},
		{"POST", dflt + "/secrets", `{"metadata":{"name":"db3"},"stringData":{"password":"hunter2"}}`, http.StatusCreated},
		{"DELETE", dflt + "/secrets/db", "", http.StatusOK},
		{"DELETE", dflt + "/secrets/db", "", http.StatusNotFound},

		// ReplicaSets
		{"POST", apps + "/replicasets", `{"metadata":{"name":"web"},"spec":{"replicas":2,` + template + `}}`, http.StatusCreated},
		{"POST", apps + "/replicasets", `{"metadata":{"name":"bad"},"spec":{"replicas":-1,"template":{"spec":{"containers":[{"name":"c"}]}}}}`, http.StatusBadRequest},
		{"POST", apps + "/replicasets", `{"metadata":{"name":"empty"},"spec":{"replicas":1}}`, http.StatusBadRequest},
		{"POST", apps + "/replicasets/web/scale", `{"replicas":5}`, http.StatusOK},
		{"POST", apps + "/replicasets/nope/scale", `{"replicas":5}`, http.StatusNotFound},
		{"DELETE", apps + "/replicasets/web", "", http.StatusOK},

		// Deployments
		{"POST", apps + "/deployments", `{"metadata":{"name":"app"},"spec":{"replicas":2,"template":{"metadata":{"labels":{"app":"x"}},"spec":{` + nginx + `}}}}`, http.StatusCreated},
		{"POST", apps + "/deployments", `{"metadata":{"name":"app"},"spec":{"replicas":2,"template":{"metadata":{"labels":{"app":"x"}},"spec":{` + nginx + `}}}}`, http.StatusConflict},
		{"POST", apps + "/deployments", `{"metadata":{"name":"nolabels"},"spec":{"replicas":1,` + template + `}}`, http.StatusBadRequest},
		{"PUT", apps + "/deployments/app", `{"spec":{"replicas":2,"template":{"metadata":{"labels":{"app":"x"}},"spec":{"containers":[{"name":"c","image":"nginx:2"}]}}}}`, http.StatusOK},
		{"PUT", apps + "/deployments/nope", `{"spec":{"replicas":2,"template":{"metadata":{"labels":{"app":"x"}},"spec":{` + nginx + `}}}}`, http.StatusNotFound},
		{"POST", apps + "/deployments/app/scale", `{"replicas":4}`, http.StatusOK},
		{"DELETE", apps + "/deployments/app", "", http.StatusOK},

		// Services
		{"POST", dflt + "/services", `{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"},"ports":[{"port":8081,"targetPort":80}]}}`, http.StatusCreated},
		{"POST", dflt + "/services", `{"metadata":{"name":"other"},"spec":{"selector":{"app":"web"},"ports":[{"port":8081}]}}`, http.StatusConflict},
		{"POST", dflt + "/services", `{"metadata":{"name":"bad"},"spec":{"selector":{"app":"web"},"ports":[{"port":99999}]}}`, http.StatusBadRequest},
		{"POST", dflt + "/services", `{"metadata":{"name":"noports"},"spec":{"selector":{"app":"web"}}}`, http.StatusBadRequest},
		{"POST", dflt + "/services", `{"metadata":{"name":"all"},"spec":{"ports":[{"port":8082}]}}`, http.StatusBadRequest},
		{"DELETE", dflt + "/services/web", "", http.StatusOK},

		// Events
		{"POST", core + "/events", `{"kind":"Pod","namespace":"default","name":"nginx","type":"Normal","reason":"Scheduled","message":"hi"}`, http.StatusCreated},
		{"POST", core + "/events", `{"kind":"Pod","name":"nginx","type":"Fine","reason":"Scheduled"}`, http.StatusBadRequest},
		{"POST", core + "/events", `{"type":"Normal","reason":"Scheduled"}`, http.StatusBadRequest},
		{"GET", core + "/events?namespace=default&kind=Pod&name=nginx", "", http.StatusOK},

		// Deleting
		{"DELETE", dflt + "/pods/nginx", "", http.StatusOK},
		{"DELETE", dflt + "/pods/nginx", "", http.StatusNotFound},

		// Wrong method on a known path
		{"PATCH", core + "/pods", "", http.StatusMethodNotAllowed},
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

// TestCreatedObjectHasKubernetesShape checks what the API server sends back
// for a new object: kind and apiVersion, and the metadata it fills in.
func TestCreatedObjectHasKubernetesShape(t *testing.T) {
	handler := NewHandler(store.New())

	req := httptest.NewRequest("POST", "/apis/apps/v1/namespaces/default/deployments",
		strings.NewReader(`{"metadata":{"name":"app"},"spec":{"replicas":1,"template":{"metadata":{"labels":{"app":"x"}},"spec":{`+nginx+`}}}}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("got status %d: %s", rec.Code, rec.Body)
	}

	var d api.Deployment
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d.APIVersion != "apps/v1" || d.Kind != "Deployment" {
		t.Errorf("got apiVersion %q kind %q, want apps/v1 Deployment", d.APIVersion, d.Kind)
	}
	if d.Namespace != "default" || d.UID == "" || d.ResourceVersion == "" || d.CreationTimestamp.IsZero() {
		t.Errorf("metadata not filled in: %+v", d.ObjectMeta)
	}
}

func TestLimitBecomesRequest(t *testing.T) {
	st := store.New()
	handler := NewHandler(st)

	req := httptest.NewRequest("POST", "/api/v1/namespaces/default/pods",
		strings.NewReader(pod("sized", `"containers":[{"name":"c","resources":{"limits":{"memory":"64Mi"}}}]`)))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("got status %d: %s", rec.Code, rec.Body)
	}

	p, _ := st.GetPod("default", "sized")
	if got := p.Containers[0].Resources.Requests["memory"]; got != "64Mi" {
		t.Errorf("memory request: got %q, want the limit, 64Mi", got)
	}
}

func TestLabelSelector(t *testing.T) {
	st := store.New()
	handler := NewHandler(st)
	for name, app := range map[string]string{"a": "web", "b": "web", "c": "db"} {
		st.CreatePod(api.Pod{ObjectMeta: api.ObjectMeta{Name: name, Namespace: "default", Labels: api.Labels{"app": app}}})
	}

	list := func(query string) (int, []api.Pod) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/pods?labelSelector="+query, nil))
		var pods []api.Pod
		json.Unmarshal(rec.Body.Bytes(), &pods)
		return rec.Code, pods
	}

	if code, pods := list("app%3Dweb"); code != http.StatusOK || len(pods) != 2 {
		t.Errorf("app=web: got status %d and %d pods, want 2", code, len(pods))
	}
	if _, pods := list("app%21%3Dweb"); len(pods) != 1 || pods[0].Name != "c" {
		t.Errorf("app!=web: got %v, want only c", pods)
	}
	if code, _ := list("%3Dweb"); code != http.StatusBadRequest {
		t.Errorf("a broken selector: got status %d, want 400", code)
	}
}
