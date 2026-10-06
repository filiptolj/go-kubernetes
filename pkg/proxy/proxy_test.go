package proxy

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// fakePod starts a web server that answers with the pod's name, and returns
// its host:port.
// ns is the namespace the tests use.
const ns = api.DefaultNamespace

func fakePod(t *testing.T, name string) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, name)
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// freePort returns a TCP port nobody is using.
func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// noKeepAlive is an HTTP client that opens a new connection for every
// request. The proxy balances connections, not requests: with keep-alive,
// several requests would share one connection and so reach the same pod.
var noKeepAlive = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

// get fetches the service on port and returns the answer.
func get(t *testing.T, port int) string {
	t.Helper()

	resp, err := noKeepAlive.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err != nil {
		t.Fatalf("GET through the proxy: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func TestProxy(t *testing.T) {
	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	defer ts.Close()

	p := &Proxy{Client: client.New(ts.URL), BindAddr: "127.0.0.1"}
	defer p.closeAll()

	// Two web pods, one pod of another app, and one web pod that isn't running.
	web := api.Labels{"app": "web"}
	st.CreatePod(api.Pod{Namespace: ns, Name: "web-a", Labels: web, Phase: api.PodRunning, Ready: true, Address: fakePod(t, "web-a")})
	st.CreatePod(api.Pod{Namespace: ns, Name: "web-b", Labels: web, Phase: api.PodRunning, Ready: true, Address: fakePod(t, "web-b")})
	st.CreatePod(api.Pod{Namespace: ns, Name: "db", Labels: api.Labels{"app": "db"}, Phase: api.PodRunning, Ready: true, Address: fakePod(t, "db")})
	st.CreatePod(api.Pod{Namespace: ns, Name: "web-pending", Labels: web, Phase: api.PodPending, Address: fakePod(t, "web-pending")})
	st.CreatePod(api.Pod{Namespace: ns, Name: "web-starting", Labels: web, Phase: api.PodRunning, Ready: false, Address: fakePod(t, "web-starting")})
	st.CreateNamespace(api.Namespace{Name: "other"})
	st.CreatePod(api.Pod{Namespace: "other", Name: "web-elsewhere", Labels: web, Phase: api.PodRunning, Ready: true, Address: fakePod(t, "web-elsewhere")})

	port := freePort(t)
	st.CreateService(api.Service{Namespace: ns, Name: "web", Port: port, Selector: web})
	p.sync()

	// Connections take turns between the two ready web pods, and never reach
	// the db pod, the pending one, the one that isn't ready yet, or the web
	// pod in another namespace.
	var answers []string
	for range 4 {
		answers = append(answers, get(t, port))
	}
	want := "web-a web-b web-a web-b"
	if got := strings.Join(answers, " "); got != want {
		t.Errorf("answers: got %q, want %q", got, want)
	}

	// A web pod stops: it no longer gets connections.
	st.SetPodStatus(ns, "web-a", api.PodStatus{Phase: api.PodFailed})
	p.sync()
	for range 2 {
		if got := get(t, port); got != "web-b" {
			t.Errorf("after web-a failed: got %q, want web-b", got)
		}
	}

	// The service is deleted: the proxy stops listening.
	st.DeleteService(ns, "web")
	p.sync()
	_, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err == nil {
		t.Error("the proxy still accepts connections for a deleted service")
	}
}

func TestLabelsMatches(t *testing.T) {
	pod := api.Labels{"app": "web", "tier": "frontend"}

	tests := []struct {
		selector api.Labels
		want     bool
	}{
		{api.Labels{"app": "web"}, true},
		{api.Labels{"app": "web", "tier": "frontend"}, true},
		{api.Labels{"app": "db"}, false},
		{api.Labels{"app": "web", "tier": "backend"}, false},
		{api.Labels{}, false}, // an empty selector selects nothing
	}

	for _, tt := range tests {
		if got := pod.Matches(tt.selector); got != tt.want {
			t.Errorf("%v.Matches(%v) = %t, want %t", pod, tt.selector, got, tt.want)
		}
	}
}
