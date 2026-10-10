package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// ingress returns an Ingress with one rule.
func ingress(name, host string, paths ...api.IngressPath) api.Ingress {
	return api.Ingress{
		ObjectMeta:  api.ObjectMeta{Name: name, Namespace: ns},
		IngressSpec: api.IngressSpec{Rules: []api.IngressRule{{Host: host, HTTP: api.IngressRuleValue{Paths: paths}}}},
	}
}

// to returns an Ingress path to a Service's port.
func to(path, pathType, service string, port int) api.IngressPath {
	return api.IngressPath{Path: path, PathType: pathType, Backend: api.IngressBackend{Service: api.IngressServiceBackend{
		Name: service, Port: api.ServiceBackendPort{Number: port},
	}}}
}

func TestRoute(t *testing.T) {
	ingresses := []api.Ingress{
		ingress("shop", "shop.local", to("/", "Prefix", "web", 80), to("/api", "Prefix", "api", 80), to("/health", "Exact", "health", 80)),
		ingress("any", "", to("/", "Prefix", "fallback", 80)),
	}

	tests := []struct{ host, path, want string }{
		{"shop.local", "/", "web"},
		{"SHOP.local:8090", "/cart", "web"},
		{"shop.local", "/api", "api"},
		{"shop.local", "/api/users", "api"},
		{"shop.local", "/apis", "web"}, // not under /api: prefixes match whole segments
		{"shop.local", "/health", "health"},
		{"shop.local", "/health/deep", "web"}, // Exact
		{"other.local", "/api", "fallback"},
	}
	for _, tt := range tests {
		backend, _, ok := route(ingresses, tt.host, tt.path)
		if !ok || backend.Name != tt.want {
			t.Errorf("%s%s: got %q (found: %t), want %q", tt.host, tt.path, backend.Name, ok, tt.want)
		}
	}

	if _, _, ok := route(ingresses[:1], "other.local", "/"); ok {
		t.Error("a host without rules was routed")
	}
}

func TestIngressServesThroughServices(t *testing.T) {
	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	defer ts.Close()

	ingressAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	p := &Proxy{Client: client.New(ts.URL), BindAddr: "127.0.0.1", IngressAddr: ingressAddr}
	defer p.closeAll()

	st.CreatePod(pod(t, ns, "web-a", api.Labels{"app": "web"}, api.PodRunning, true))
	st.CreatePod(pod(t, ns, "api-a", api.Labels{"app": "api"}, api.PodRunning, true))
	webPort, apiPort := freePort(t), freePort(t)
	for name, port := range map[string]int{"web": webPort, "api": apiPort} {
		st.CreateService(api.Service{
			ObjectMeta:  api.ObjectMeta{Name: name, Namespace: ns},
			ServiceSpec: api.ServiceSpec{Selector: api.Labels{"app": name}, Ports: []api.ServicePort{{Port: port, TargetPort: 80}}},
		})
	}
	st.Ingresses.Create(ingress("shop", "shop.local", to("/", "Prefix", "web", webPort), to("/api", "Prefix", "api", apiPort)))
	p.sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.serveIngress(ctx, ingressAddr)

	get := func(host, path string) (int, string) {
		req, _ := http.NewRequest("GET", "http://"+ingressAddr+path, nil)
		req.Host = host
		var resp *http.Response
		var err error
		for range 50 { // the server may take a moment to start
			resp, err = http.DefaultClient.Do(req)
			if err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	if code, body := get("shop.local", "/"); code != 200 || body != "web-a" {
		t.Errorf("shop.local/: got %d %q, want web-a", code, body)
	}
	if code, body := get("shop.local", "/api/orders"); code != 200 || body != "api-a" {
		t.Errorf("shop.local/api/orders: got %d %q, want api-a", code, body)
	}
	if code, _ := get("nowhere.local", "/"); code != http.StatusNotFound {
		t.Errorf("an unknown host: got %d, want 404", code)
	}
}

func TestEndpointsByPodIP(t *testing.T) {
	svc := api.Service{
		ObjectMeta:  api.ObjectMeta{Name: "web", Namespace: ns},
		ServiceSpec: api.ServiceSpec{Selector: api.Labels{"app": "web"}},
	}
	port := api.ServicePort{Port: 8081, TargetPort: 80}
	pods := []api.Pod{{
		ObjectMeta: api.ObjectMeta{Name: "web-a", Namespace: ns, Labels: api.Labels{"app": "web"}},
		PodStatus:  api.PodStatus{Phase: api.PodRunning, Ready: true, PodIP: "10.244.128.5", HostPorts: map[int]string{80: "127.0.0.1:40000"}},
	}}

	if eps := endpointsFor(svc, port, pods, true); len(eps) != 1 || eps[0].Address != "10.244.128.5:80" {
		t.Errorf("by pod IP: got %v, want 10.244.128.5:80", eps)
	}
	if eps := endpointsFor(svc, port, pods, false); len(eps) != 1 || eps[0].Address != "127.0.0.1:40000" {
		t.Errorf("by host port: got %v, want 127.0.0.1:40000", eps)
	}
}
