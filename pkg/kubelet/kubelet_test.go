package kubelet

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/cri"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// fakeRuntime is a cri.Runtime that runs nothing. The test decides when each
// "container" exits, and with what result, by calling exit.
type fakeRuntime struct {
	mu        sync.Mutex
	running   map[string]chan error // by "pod/container" (the tests use one namespace)
	addresses map[string]string     // address to report, by pod; "fake:<pod>" if not set
}

// ns is the namespace the tests use.
const ns = api.DefaultNamespace

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{running: make(map[string]chan error), addresses: make(map[string]string)}
}

func (f *fakeRuntime) Start(pod api.Pod, c api.Container, logs io.Writer) (cri.Running, error) {
	podName := pod.Name
	f.mu.Lock()
	defer f.mu.Unlock()

	done := make(chan error, 1)
	f.running[podName+"/"+c.Name] = done

	io.WriteString(logs, "hello from "+podName+"\n")

	address, ok := f.addresses[podName]
	if !ok {
		address = "fake:" + podName
	}
	return cri.Running{Done: done, Address: address}, nil
}

func (f *fakeRuntime) Stop(pod api.Pod, c api.Container) error {
	f.exit(pod.Name+"/"+c.Name, errors.New("signal: killed"))
	return nil
}

func (f *fakeRuntime) RemoveAll() error {
	return nil
}

// exit makes a running fake container finish with result.
func (f *fakeRuntime) exit(key string, result error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	done, ok := f.running[key]
	if ok {
		delete(f.running, key)
		done <- result
	}
}

// isRunning reports whether a fake container is running.
func (f *fakeRuntime) isRunning(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.running[key]
	return ok
}

// waitFor checks condition until it is true, and fails the test if that
// takes longer than two seconds.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fetch GETs path from node-1's kubelet, at the address it registered, and
// returns the body.
func fetch(t *testing.T, st *store.Store, path string) string {
	t.Helper()

	nodes := st.ListNodes()
	if len(nodes) == 0 || nodes[0].Address == "" {
		t.Fatal("node-1 registered no address")
	}

	resp, err := http.Get(nodes[0].Address + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

// phaseIs returns a condition that is true once the pod has the given phase.
func phaseIs(st *store.Store, name string, phase api.PodPhase) func() bool {
	return func() bool {
		pod, _ := st.GetPod(ns, name)
		return pod.Phase == phase
	}
}

func TestKubelet(t *testing.T) {
	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	defer ts.Close()

	rt := newFakeRuntime()
	k := New(Config{
		NodeName:   "node-1",
		LogDir:     t.TempDir(),
		Heartbeat:  50 * time.Millisecond,
		ListenAddr: "127.0.0.1:0", // port 0: the system picks a free one
	}, client.New(ts.URL), rt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopped := make(chan error)
	go func() {
		stopped <- k.Run(ctx)
	}()

	waitFor(t, "node-1 to register", func() bool {
		nodes := st.ListNodes()
		return len(nodes) == 1 && nodes[0].Ready
	})

	// newPod creates a pod with one container and binds it to node-1, the way
	// the scheduler would.
	newPod := func(name string) {
		st.CreatePod(api.Pod{
			Name:       name,
			Namespace:  ns,
			Containers: []api.Container{{Name: "main", Image: "busybox"}},
			Phase:      api.PodPending,
		})
		st.BindPod(ns, name, "node-1")
	}

	t.Run("container exits cleanly", func(t *testing.T) {
		newPod("ok")
		waitFor(t, "pod ok to be Running", phaseIs(st, "ok", api.PodRunning))

		pod, _ := st.GetPod(ns, "ok")
		if pod.Address != "fake:ok" {
			t.Errorf("pod address: got %q, want %q", pod.Address, "fake:ok")
		}
		if pod.Ready {
			t.Error("pod ok is ready, but nothing answers at its address")
		}

		logs := fetch(t, st, "/logs/default/ok/main")
		if logs != "hello from ok\n" {
			t.Errorf("logs: got %q, want %q", logs, "hello from ok\n")
		}

		rt.exit("ok/main", nil)
		waitFor(t, "pod ok to be Succeeded", phaseIs(st, "ok", api.PodSucceeded))
	})

	t.Run("pod becomes ready when its port answers", func(t *testing.T) {
		// A listener that accepts connections and keeps them open, like a
		// web server waiting for a request.
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
			}
		}()

		rt.mu.Lock()
		rt.addresses["server"] = l.Addr().String()
		rt.mu.Unlock()

		newPod("server")
		waitFor(t, "pod server to be ready", func() bool {
			pod, _ := st.GetPod(ns, "server")
			return pod.Ready
		})
		rt.exit("server/main", nil)
	})

	t.Run("container crashes", func(t *testing.T) {
		newPod("crash")
		waitFor(t, "pod crash to be Running", phaseIs(st, "crash", api.PodRunning))

		rt.exit("crash/main", errors.New("exit status 1"))
		waitFor(t, "pod crash to be Failed", phaseIs(st, "crash", api.PodFailed))
	})

	t.Run("pod is deleted", func(t *testing.T) {
		newPod("doomed")
		waitFor(t, "pod doomed to be Running", phaseIs(st, "doomed", api.PodRunning))

		st.DeletePod(ns, "doomed")
		waitFor(t, "container of doomed to be stopped", func() bool {
			return !rt.isRunning("doomed/main")
		})
	})

	t.Run("pods on other nodes are ignored", func(t *testing.T) {
		st.PutNode(api.Node{Name: "node-2", Ready: true})
		st.CreatePod(api.Pod{Namespace: ns, Name: "elsewhere", Containers: []api.Container{{Name: "main"}}, Phase: api.PodPending})
		st.BindPod(ns, "elsewhere", "node-2")

		time.Sleep(100 * time.Millisecond)
		if rt.isRunning("elsewhere/main") {
			t.Error("node-1's kubelet started a pod bound to node-2")
		}
	})

	// Shutting down stops the running pods and marks the node NotReady.
	newPod("last")
	waitFor(t, "pod last to be Running", phaseIs(st, "last", api.PodRunning))

	cancel()
	err := <-stopped
	if err != nil {
		t.Fatalf("Run returned an error after cancel: %v", err)
	}

	pod, _ := st.GetPod(ns, "last")
	if pod.Phase != api.PodFailed {
		t.Errorf("after shutdown: pod last is %s, want Failed", pod.Phase)
	}
	for _, node := range st.ListNodes() {
		if node.Name == "node-1" && node.Ready {
			t.Error("after shutdown: node-1 is still Ready")
		}
	}
}

func TestSafeName(t *testing.T) {
	tests := map[string]bool{
		"web-x7k2p": true,
		"main":      true,
		"":          false,
		".":         false,
		"..":        false,
		"a/b":       false,
		`a\b`:       false,
	}

	for name, want := range tests {
		if got := safeName(name); got != want {
			t.Errorf("safeName(%q) = %t, want %t", name, got, want)
		}
	}
}

func TestProbe(t *testing.T) {
	// Nothing listening at all.
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	closedAddr := l.Addr().String()
	l.Close()
	if probe(closedAddr) {
		t.Error("probe of a closed port: got ready")
	}

	// Accepts, then hangs up at once: what Docker does before the program
	// inside the container is listening.
	hangUp, _ := net.Listen("tcp", "127.0.0.1:0")
	defer hangUp.Close()
	go func() {
		for {
			conn, err := hangUp.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	if probe(hangUp.Addr().String()) {
		t.Error("probe of a port that hangs up at once: got ready")
	}

	// A real server, waiting for us to speak.
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	if !probe(strings.TrimPrefix(srv.URL, "http://")) {
		t.Error("probe of a web server: got not ready")
	}
}
