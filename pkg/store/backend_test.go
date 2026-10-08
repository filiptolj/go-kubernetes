package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

// testBackend checks that a store saved in b comes back the same after a
// restart. open must return a fresh Backend connected to the same storage,
// as if the API server had restarted. Every Backend must pass this test.
func testBackend(t *testing.T, open func() Backend) {
	first, err := Open(open())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	first.PutNode(api.Node{ObjectMeta: meta("node-1"), NodeStatus: api.NodeStatus{Ready: true}})
	first.CreatePod(api.Pod{ObjectMeta: meta("nginx")})
	first.BindPod(ns, "nginx", "node-1")
	first.CreatePod(api.Pod{ObjectMeta: meta("gone")})
	first.DeletePod(ns, "gone")
	first.CreateReplicaSet(api.ReplicaSet{ObjectMeta: meta("web"), ReplicaSetSpec: api.ReplicaSetSpec{Replicas: 3}})
	first.CreateDeployment(api.Deployment{ObjectMeta: meta("app"), DeploymentSpec: api.DeploymentSpec{Replicas: 2}})
	first.CreateService(service("svc", 8081, api.Labels{"app": "web"}, ns))
	first.Jobs.Create(api.Job{ObjectMeta: meta("report"), JobSpec: api.JobSpec{Completions: 2}})
	first.ConfigMaps.Create(api.ConfigMap{ObjectMeta: meta("settings"), Data: map[string]string{"mode": "fast"}})
	first.Close()

	second, err := Open(open())
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer second.Close()

	pod, ok := second.GetPod(ns, "nginx")
	if !ok || pod.NodeName != "node-1" {
		t.Errorf("after reopening: got pod %+v (found: %t), want nginx bound to node-1", pod, ok)
	}
	if _, ok := second.GetPod(ns, "gone"); ok {
		t.Error("after reopening: a deleted pod came back")
	}
	if n := len(second.ListNodes()); n != 1 {
		t.Errorf("after reopening: got %d nodes, want 1", n)
	}
	sets := second.ListReplicaSets("")
	if len(sets) != 1 || sets[0].Replicas != 3 {
		t.Errorf("after reopening: got replicasets %+v, want web with 3 replicas", sets)
	}
	if n := len(second.ListDeployments("")); n != 1 {
		t.Errorf("after reopening: got %d deployments, want 1", n)
	}
	if svcs := second.ListServices(""); len(svcs) != 1 || svcs[0].Selector["app"] != "web" {
		t.Errorf("after reopening: got services %+v, want svc selecting app=web", svcs)
	}
	if job, ok := second.Jobs.Get(ns, "report"); !ok || job.Completions != 2 {
		t.Errorf("after reopening: got job %+v (found: %t), want report with 2 completions", job, ok)
	}
	if cm, ok := second.ConfigMaps.Get(ns, "settings"); !ok || cm.Data["mode"] != "fast" {
		t.Errorf("after reopening: got configmap %+v (found: %t), want settings with mode=fast", cm, ok)
	}
}

func TestFileBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "state.json")

	testBackend(t, func() Backend {
		b, err := OpenFile(path)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		return b
	})
}

// TestEtcdBackend needs a running etcd, so it only runs when MINIK8S_ETCD
// says where to find one:
//
//	MINIK8S_ETCD=localhost:2379 go test ./pkg/store
func TestEtcdBackend(t *testing.T) {
	endpoints := os.Getenv("MINIK8S_ETCD")
	if endpoints == "" {
		t.Skip("set MINIK8S_ETCD=host:port to run this test against a real etcd")
	}

	open := func() Backend {
		b, err := OpenEtcd(strings.Split(endpoints, ","))
		if err != nil {
			t.Fatalf("OpenEtcd: %v", err)
		}
		return b
	}

	// Start from an empty etcd, and leave it empty afterwards.
	clear := func() {
		b := open()
		defer b.Close()
		all, _ := b.LoadAll()
		for kind, objects := range all {
			for name := range objects {
				b.Delete(kind, name)
			}
		}
	}
	clear()
	t.Cleanup(clear)

	testBackend(t, open)
}

// brokenBackend fails every write, like an etcd that went down.
type brokenBackend struct{}

// LoadAll succeeds, with the default namespace already saved, so that Open
// has nothing to write and only the test's own change hits the failure.
func (brokenBackend) LoadAll() (map[string]map[string][]byte, error) {
	return map[string]map[string][]byte{
		kindNamespaces: {api.DefaultNamespace: []byte(`{"metadata":{"name":"default"}}`)},
	}, nil
}
func (brokenBackend) Put(kind, name string, data []byte) error { return errors.New("disk on fire") }
func (brokenBackend) Delete(kind, name string) error           { return errors.New("disk on fire") }
func (brokenBackend) Close() error                             { return nil }

func TestBackendFailureRefusesTheChange(t *testing.T) {
	s, err := Open(brokenBackend{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.CreatePod(api.Pod{ObjectMeta: meta("nginx")})
	if err == nil {
		t.Fatal("CreatePod succeeded although the backend couldn't save it")
	}
	if _, ok := s.GetPod(ns, "nginx"); ok {
		t.Error("the pod is in memory although it was never saved")
	}
}
