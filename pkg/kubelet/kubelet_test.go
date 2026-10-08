package kubelet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
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

// ns is the namespace the tests use.
const ns = api.DefaultNamespace

// fakeRuntime is a cri.Runtime that runs nothing. The test decides when each
// "container" exits, and with what result, by calling exit.
type fakeRuntime struct {
	mu        sync.Mutex
	running   map[string]chan error  // by "pod/container"
	starts    map[string]int         // how often each container was started
	options   map[string]cri.Options // what each container last started with
	addresses map[string]string      // address to report, by pod; "fake:<pod>" if not set
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{
		running:   make(map[string]chan error),
		starts:    make(map[string]int),
		options:   make(map[string]cri.Options),
		addresses: make(map[string]string),
	}
}

func (f *fakeRuntime) Start(pod api.Pod, c api.Container, opts cri.Options) (cri.Running, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := pod.Name + "/" + c.Name
	done := make(chan error, 1)
	f.running[key] = done
	f.starts[key]++
	f.options[key] = opts

	io.WriteString(opts.Logs, "hello from "+pod.Name+"\n")

	address, ok := f.addresses[pod.Name]
	if !ok {
		address = "fake:" + pod.Name
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

// startCount returns how often a fake container has been started.
func (f *fakeRuntime) startCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts[key]
}

// waitFor checks condition until it is true, and fails the test if that
// takes longer than five seconds.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
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

// testNode is a kubelet for node-1, with an API server in memory.
type testNode struct {
	t     *testing.T
	st    *store.Store
	rt    *fakeRuntime
	cfg   Config
	stop  context.CancelFunc
	ended chan error
}

// startNode starts a kubelet for node-1 and waits until it has registered.
func startNode(t *testing.T) *testNode {
	t.Helper()

	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	t.Cleanup(ts.Close)

	n := &testNode{t: t, st: st, rt: newFakeRuntime(), ended: make(chan error, 1)}
	n.cfg = Config{
		NodeName:        "node-1",
		LogDir:          t.TempDir(),
		VolumeDir:       t.TempDir(),
		Heartbeat:       50 * time.Millisecond,
		ListenAddr:      "127.0.0.1:0", // port 0: the system picks a free one
		RestartDelay:    20 * time.Millisecond,
		MaxRestartDelay: 100 * time.Millisecond,
		HealthyAfter:    time.Hour,
		Resync:          50 * time.Millisecond,
	}
	k := New(n.cfg, client.New(ts.URL), n.rt)

	ctx, cancel := context.WithCancel(context.Background())
	n.stop = cancel
	go func() {
		n.ended <- k.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-n.ended
	})

	waitFor(t, "node-1 to register", func() bool {
		nodes := st.ListNodes()
		return len(nodes) == 1 && nodes[0].Ready
	})
	return n
}

// run creates a pod with one container called "main", bound to node-1, the
// way the scheduler would. change can adjust the pod first.
func (n *testNode) run(name string, change func(*api.Pod)) {
	n.t.Helper()

	pod := api.Pod{
		Name:          name,
		Namespace:     ns,
		Containers:    []api.Container{{Name: "main", Image: "busybox"}},
		RestartPolicy: api.RestartAlways,
		Phase:         api.PodPending,
	}
	if change != nil {
		change(&pod)
	}

	err := n.st.CreatePod(pod)
	if err != nil {
		n.t.Fatal(err)
	}
	_, err = n.st.BindPod(ns, name, "node-1")
	if err != nil {
		n.t.Fatal(err)
	}
}

// pod returns a pod as the API server sees it.
func (n *testNode) pod(name string) api.Pod {
	pod, _ := n.st.GetPod(ns, name)
	return pod
}

// waitPhase waits until a pod has the given phase.
func (n *testNode) waitPhase(name string, phase api.PodPhase) {
	n.t.Helper()
	waitFor(n.t, fmt.Sprintf("pod %s to be %s", name, phase), func() bool {
		return n.pod(name).Phase == phase
	})
}

// never sets a pod's restart policy to Never.
func never(p *api.Pod) { p.RestartPolicy = api.RestartNever }

func TestContainerExitsCleanly(t *testing.T) {
	n := startNode(t)
	n.run("ok", never)
	n.waitPhase("ok", api.PodRunning)

	// A container without a port is ready as soon as it runs.
	waitFor(t, "pod ok to be ready", func() bool { return n.pod("ok").Ready })

	if logs := fetch(t, n.st, "/logs/default/ok/main"); logs != "hello from ok\n" {
		t.Errorf("logs: got %q, want %q", logs, "hello from ok\n")
	}

	n.rt.exit("ok/main", nil)
	n.waitPhase("ok", api.PodSucceeded)
}

func TestContainerCrashesWithRestartNever(t *testing.T) {
	n := startNode(t)
	n.run("crash", never)
	n.waitPhase("crash", api.PodRunning)

	n.rt.exit("crash/main", errors.New("exit status 1"))
	n.waitPhase("crash", api.PodFailed)

	if starts := n.rt.startCount("crash/main"); starts != 1 {
		t.Errorf("container started %d times, want 1: Never means no restarts", starts)
	}
}

func TestRestartAlways(t *testing.T) {
	n := startNode(t)
	n.run("loop", nil)
	n.waitPhase("loop", api.PodRunning)

	// It crashes twice: each time it comes back, and the restarts are counted.
	for i := 1; i <= 2; i++ {
		waitFor(t, "the container to run", func() bool { return n.rt.isRunning("loop/main") })
		n.rt.exit("loop/main", errors.New("exit status 1"))
		waitFor(t, fmt.Sprintf("restart %d", i), func() bool { return n.pod("loop").Restarts == i })
	}

	// Even a clean exit is restarted under Always.
	waitFor(t, "the container to run", func() bool { return n.rt.isRunning("loop/main") })
	n.rt.exit("loop/main", nil)
	waitFor(t, "restart 3", func() bool { return n.pod("loop").Restarts == 3 })

	if phase := n.pod("loop").Phase; phase != api.PodRunning {
		t.Errorf("phase: got %s, want Running: an Always pod never finishes on its own", phase)
	}
	if n.st.ListEvents(ns, "Pod", "loop") == nil {
		t.Error("expected events about the restarts")
	}
}

func TestRestartOnFailure(t *testing.T) {
	n := startNode(t)
	n.run("job", func(p *api.Pod) { p.RestartPolicy = api.RestartOnFailure })
	n.waitPhase("job", api.PodRunning)

	n.rt.exit("job/main", errors.New("exit status 1"))
	waitFor(t, "a restart after the failure", func() bool { return n.pod("job").Restarts == 1 })

	waitFor(t, "the container to run again", func() bool { return n.rt.isRunning("job/main") })
	n.rt.exit("job/main", nil)
	n.waitPhase("job", api.PodSucceeded)
}

func TestPodBecomesReadyWhenItsPortAnswers(t *testing.T) {
	n := startNode(t)

	// A listener that accepts connections and keeps them open, like a web
	// server waiting for a request.
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

	n.rt.mu.Lock()
	n.rt.addresses["server"] = l.Addr().String()
	n.rt.mu.Unlock()

	n.run("server", func(p *api.Pod) { p.Containers[0].Port = 80 })
	waitFor(t, "pod server to be ready", func() bool { return n.pod("server").Ready })

	if addr := n.pod("server").Address; addr != l.Addr().String() {
		t.Errorf("address: got %q, want %q", addr, l.Addr().String())
	}
}

func TestLivenessProbeRestartsAnUnhealthyContainer(t *testing.T) {
	n := startNode(t)

	// A web server that always answers 500 Internal Server Error.
	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not feeling well", http.StatusInternalServerError)
	}))
	defer sick.Close()

	n.rt.mu.Lock()
	n.rt.addresses["sick"] = strings.TrimPrefix(sick.URL, "http://")
	n.rt.mu.Unlock()

	n.run("sick", func(p *api.Pod) {
		p.Containers[0].Port = 80
		p.Containers[0].LivenessProbe = &api.Probe{
			HTTPGet:          &api.HTTPGetAction{Path: "/healthz"},
			PeriodSeconds:    1,
			FailureThreshold: 1,
		}
	})

	waitFor(t, "the liveness probe to kill and restart the container", func() bool {
		return n.rt.startCount("sick/main") >= 2
	})

	var unhealthy bool
	for _, e := range n.st.ListEvents(ns, "Pod", "sick") {
		unhealthy = unhealthy || e.Reason == "Unhealthy"
	}
	if !unhealthy {
		t.Error("expected an Unhealthy event")
	}
}

func TestEnvironmentAndVolumes(t *testing.T) {
	n := startNode(t)
	n.st.ConfigMaps.Create(api.ConfigMap{
		Meta: api.Meta{Name: "settings", Namespace: ns},
		Data: map[string]string{"mode": "fast", "app.conf": "color=blue\n"},
	})
	n.st.Secrets.Create(api.Secret{
		Meta: api.Meta{Name: "db", Namespace: ns},
		Data: map[string]string{"password": "hunter2"},
	})

	n.run("app", func(p *api.Pod) {
		p.Volumes = []api.Volume{
			{Name: "config", ConfigMap: &api.ObjectRef{Name: "settings"}},
			{Name: "scratch", EmptyDir: &api.EmptyDirSource{}},
		}
		p.Containers[0].Env = []api.EnvVar{
			{Name: "GREETING", Value: "hi"},
			{Name: "MODE", ValueFrom: &api.EnvSource{ConfigMapKeyRef: &api.KeyRef{Name: "settings", Key: "mode"}}},
			{Name: "DB_PASSWORD", ValueFrom: &api.EnvSource{SecretKeyRef: &api.KeyRef{Name: "db", Key: "password"}}},
		}
		p.Containers[0].VolumeMounts = []api.VolumeMount{
			{Name: "config", MountPath: "/etc/app"},
			{Name: "scratch", MountPath: "/tmp/work"},
		}
	})
	waitFor(t, "the container to start", func() bool { return n.rt.isRunning("app/main") })

	n.rt.mu.Lock()
	opts := n.rt.options["app/main"]
	n.rt.mu.Unlock()

	for _, want := range []string{"GREETING=hi", "MODE=fast", "DB_PASSWORD=hunter2", "POD_NAME=app", "NODE_NAME=node-1"} {
		if !slices.Contains(opts.Env, want) {
			t.Errorf("environment %v is missing %s", opts.Env, want)
		}
	}

	if len(opts.Mounts) != 2 || !opts.Mounts[0].ReadOnly || opts.Mounts[1].ReadOnly {
		t.Fatalf("mounts: got %+v, want a read-only config volume and a writable scratch one", opts.Mounts)
	}
	conf, err := os.ReadFile(filepath.Join(opts.Mounts[0].HostPath, "app.conf"))
	if err != nil || string(conf) != "color=blue\n" {
		t.Errorf("config file: got %q (%v), want the ConfigMap's value", conf, err)
	}

	// Deleting the pod removes the volumes the kubelet made for it.
	n.st.DeletePod(ns, "app")
	waitFor(t, "the pod's volumes to be removed", func() bool {
		_, err := os.Stat(filepath.Join(n.cfg.VolumeDir, ns, "app"))
		return errors.Is(err, os.ErrNotExist)
	})
}

func TestMissingConfigMapFailsThePod(t *testing.T) {
	n := startNode(t)
	n.run("broken", func(p *api.Pod) {
		p.Containers[0].Env = []api.EnvVar{
			{Name: "X", ValueFrom: &api.EnvSource{ConfigMapKeyRef: &api.KeyRef{Name: "nope", Key: "x"}}},
		}
	})
	n.waitPhase("broken", api.PodFailed)

	if n.rt.startCount("broken/main") != 0 {
		t.Error("the container started although its environment was incomplete")
	}
}

func TestDeletedPodIsStopped(t *testing.T) {
	n := startNode(t)
	n.run("doomed", nil)
	waitFor(t, "the container to run", func() bool { return n.rt.isRunning("doomed/main") })

	n.st.DeletePod(ns, "doomed")
	waitFor(t, "the container to be stopped", func() bool { return !n.rt.isRunning("doomed/main") })
}

func TestSameNameNewPod(t *testing.T) {
	n := startNode(t)
	n.run("db-0", nil)
	n.waitPhase("db-0", api.PodRunning)
	firstUID := n.pod("db-0").UID

	// Delete it and create a new pod with the same name, as a StatefulSet does.
	n.st.DeletePod(ns, "db-0")
	n.run("db-0", nil)

	waitFor(t, "the new db-0 to run", func() bool {
		pod := n.pod("db-0")
		return pod.UID != firstUID && pod.Phase == api.PodRunning
	})
	if starts := n.rt.startCount("db-0/main"); starts != 2 {
		t.Errorf("db-0/main started %d times, want 2: once for each pod", starts)
	}
}

func TestPodsOnOtherNodesAreIgnored(t *testing.T) {
	n := startNode(t)
	n.st.PutNode(api.Node{Name: "node-2", Ready: true})
	n.st.CreatePod(api.Pod{Namespace: ns, Name: "elsewhere", Containers: []api.Container{{Name: "main"}}, Phase: api.PodPending})
	n.st.BindPod(ns, "elsewhere", "node-2")

	time.Sleep(200 * time.Millisecond)
	if n.rt.startCount("elsewhere/main") != 0 {
		t.Error("node-1's kubelet started a pod bound to node-2")
	}
}

func TestShutdown(t *testing.T) {
	n := startNode(t)
	n.run("last", nil)
	n.waitPhase("last", api.PodRunning)

	n.stop()
	err := <-n.ended
	n.ended <- err // for the cleanup
	if err != nil {
		t.Fatalf("Run returned an error after cancel: %v", err)
	}

	if phase := n.pod("last").Phase; phase != api.PodFailed {
		t.Errorf("after shutdown: pod last is %s, want Failed", phase)
	}
	for _, node := range n.st.ListNodes() {
		if node.Ready {
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

func TestLogsBelongToOnePod(t *testing.T) {
	n := startNode(t)
	n.run("web-0", nil)
	waitFor(t, "the container to run", func() bool { return n.rt.isRunning("web-0/main") })

	logFile := filepath.Join(n.cfg.LogDir, ns, "web-0", "main.log")
	os.WriteFile(logFile, []byte("output of the first web-0\n"), 0o644)

	// A new pod with the same name starts with empty logs.
	n.st.DeletePod(ns, "web-0")
	n.run("web-0", nil)
	waitFor(t, "the new web-0 to start", func() bool { return n.rt.startCount("web-0/main") == 2 })
	if logs := fetch(t, n.st, "/logs/default/web-0/main"); logs != "hello from web-0\n" {
		t.Errorf("logs of the new pod: got %q, want only its own output", logs)
	}

	// Deleting the pod deletes its logs.
	n.st.DeletePod(ns, "web-0")
	waitFor(t, "the logs to be removed", func() bool {
		_, err := os.Stat(filepath.Dir(logFile))
		return errors.Is(err, os.ErrNotExist)
	})
}
