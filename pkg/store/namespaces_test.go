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
	s.CreateNamespace(api.Namespace{ObjectMeta: api.ObjectMeta{Name: "dev"}})

	_, err := s.CreatePod(api.Pod{ObjectMeta: metaIn("default", "web")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CreatePod(api.Pod{ObjectMeta: metaIn("dev", "web")})
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

	_, err := s.CreatePod(api.Pod{ObjectMeta: metaIn("nope", "web")})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("got error %v, want ErrNotFound", err)
	}
}

func TestDeleteNamespaceDeletesEverythingInIt(t *testing.T) {
	s := New()
	s.CreateNamespace(api.Namespace{ObjectMeta: api.ObjectMeta{Name: "dev"}})

	for _, ns := range []string{"dev", "default"} {
		s.CreatePod(api.Pod{ObjectMeta: metaIn(ns, "web-1")})
		s.CreateReplicaSet(api.ReplicaSet{ObjectMeta: metaIn(ns, "web")})
		s.CreateDeployment(api.Deployment{ObjectMeta: metaIn(ns, "app")})
		s.RecordEvent(api.Event{Namespace: ns, Kind: "Pod", Name: "web-1", Type: api.EventNormal, Reason: "Test"})
	}
	s.CreateService(service("web", 8081, nil, "dev"))
	s.Jobs.Create(api.Job{ObjectMeta: metaIn("dev", "report")})
	s.Secrets.Create(api.Secret{ObjectMeta: metaIn("dev", "db")})

	events, stop := s.Watch("pods", "")
	defer stop()

	err := s.DeleteNamespace("dev")
	if err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	// The kubelet learns about deleted pods through the watch.
	event := <-events
	if event.Type != api.EventDeleted || event.Object.(api.Pod).Namespace != "dev" {
		t.Errorf("watch: got %s %s/%s, want DELETED dev/web-1", event.Type, event.Object.(api.Pod).Namespace, event.Object.(api.Pod).Name)
	}

	if n := len(s.ListPods("dev")) + len(s.ListReplicaSets("dev")) + len(s.ListDeployments("dev")) +
		len(s.ListServices("dev")) + len(s.ListEvents("dev", "", "")) +
		len(s.Jobs.List("dev")) + len(s.Secrets.List("dev")); n != 0 {
		t.Errorf("%d objects left in the deleted namespace, want 0", n)
	}
	if n := len(s.ListPods("default")) + len(s.ListReplicaSets("default")) + len(s.ListDeployments("default")); n != 3 {
		t.Errorf("the default namespace has %d objects, want its 3 untouched", n)
	}
	if _, err := s.CreatePod(api.Pod{ObjectMeta: metaIn("dev", "x")}); !errors.Is(err, ErrNotFound) {
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

// TestOldFormatIsRefused opens a file saved before objects had
// Kubernetes' shape: the store must refuse it with a clear error instead of
// loading empty objects.
func TestOldFormatIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	old := `{"pods": [{"name": "nginx", "namespace": "default", "phase": "Running"}]}`
	os.WriteFile(path, []byte(old), 0o644)

	b, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	_, err = Open(b)
	if !errors.Is(err, ErrOldFormat) {
		t.Errorf("got error %v, want ErrOldFormat", err)
	}
}
