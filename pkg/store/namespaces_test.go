package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
)

func TestSameNameInTwoNamespaces(t *testing.T) {
	s := New()
	s.CreateNamespace(api.Namespace{Name: "dev"})

	err := s.CreatePod(api.Pod{Namespace: "default", Name: "web"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.CreatePod(api.Pod{Namespace: "dev", Name: "web"})
	if err != nil {
		t.Errorf("a pod with the same name in another namespace: %v, want no error", err)
	}

	if n := len(s.ListPods("dev")); n != 1 {
		t.Errorf("pods in dev: got %d, want 1", n)
	}
	if n := len(s.ListPods("")); n != 2 {
		t.Errorf("pods in all namespaces: got %d, want 2", n)
	}
}

func TestCreateInMissingNamespace(t *testing.T) {
	s := New()

	err := s.CreatePod(api.Pod{Namespace: "nope", Name: "web"})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("got error %v, want ErrNotFound", err)
	}
}

func TestDeleteNamespaceDeletesEverythingInIt(t *testing.T) {
	s := New()
	s.CreateNamespace(api.Namespace{Name: "dev"})

	for _, ns := range []string{"dev", "default"} {
		s.CreatePod(api.Pod{Namespace: ns, Name: "web-1"})
		s.CreateReplicaSet(api.ReplicaSet{Namespace: ns, Name: "web"})
		s.CreateDeployment(api.Deployment{Namespace: ns, Name: "app"})
		s.RecordEvent(api.Event{Namespace: ns, Kind: "Pod", Name: "web-1", Type: api.EventNormal, Reason: "Test"})
	}
	s.CreateService(api.Service{Namespace: "dev", Name: "web", Port: 8081})
	s.Jobs.Create(api.Job{Meta: api.Meta{Namespace: "dev", Name: "report"}})
	s.Secrets.Create(api.Secret{Meta: api.Meta{Namespace: "dev", Name: "db"}})

	events, stop := s.WatchPods()
	defer stop()

	err := s.DeleteNamespace("dev")
	if err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	// The kubelet learns about deleted pods through the watch.
	event := <-events
	if event.Type != api.EventDeleted || event.Pod.Namespace != "dev" {
		t.Errorf("watch: got %s %s/%s, want DELETED dev/web-1", event.Type, event.Pod.Namespace, event.Pod.Name)
	}

	if n := len(s.ListPods("dev")) + len(s.ListReplicaSets("dev")) + len(s.ListDeployments("dev")) +
		len(s.ListServices("dev")) + len(s.ListEvents("dev", "", "")) +
		len(s.Jobs.List("dev")) + len(s.Secrets.List("dev")); n != 0 {
		t.Errorf("%d objects left in the deleted namespace, want 0", n)
	}
	if n := len(s.ListPods("default")) + len(s.ListReplicaSets("default")) + len(s.ListDeployments("default")); n != 3 {
		t.Errorf("the default namespace has %d objects, want its 3 untouched", n)
	}
	if err := s.CreatePod(api.Pod{Namespace: "dev", Name: "x"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("creating in the deleted namespace: got %v, want ErrNotFound", err)
	}
}

func TestDefaultNamespaceCantBeDeleted(t *testing.T) {
	s := New()

	err := s.DeleteNamespace(api.DefaultNamespace)
	if !errors.Is(err, ErrConflict) {
		t.Errorf("got error %v, want ErrConflict", err)
	}
}

// TestMigrateOldFile opens a file saved before namespaces existed: its
// objects must move into the default namespace, in memory and in the file.
func TestMigrateOldFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	old := `{
		"pods": [{"name": "nginx", "phase": "Running", "owner": "web"}],
		"replicaSets": [{"name": "web", "replicas": 1}],
		"nodes": [{"name": "node-1", "ready": true}]
	}`
	os.WriteFile(path, []byte(old), 0o644)

	open := func() *Store {
		b, err := OpenFile(path)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		s, err := Open(b)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return s
	}

	s := open()
	pod, ok := s.GetPod(api.DefaultNamespace, "nginx")
	if !ok || pod.Namespace != api.DefaultNamespace {
		t.Fatalf("got pod %+v (found: %t), want nginx in the default namespace", pod, ok)
	}
	if sets := s.ListReplicaSets(api.DefaultNamespace); len(sets) != 1 {
		t.Errorf("got %d replicasets in default, want 1", len(sets))
	}
	if n := len(s.ListNodes()); n != 1 {
		t.Errorf("got %d nodes, want 1 (nodes have no namespace and don't move)", n)
	}

	// Opening the migrated file again must give the same result, with nothing
	// left under the old keys.
	again := open()
	if n := len(again.ListPods("")); n != 1 {
		t.Errorf("after reopening: %d pods, want 1", n)
	}
	if _, ok := again.GetPod(api.DefaultNamespace, "nginx"); !ok {
		t.Error("after reopening: nginx isn't in the default namespace")
	}
}
