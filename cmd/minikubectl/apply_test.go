package main

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

func TestSplitDocuments(t *testing.T) {
	data := `# a comment before the first document
kind: Pod
---
kind: Service
---
# only a comment: skipped
---

---
kind: Job
`
	docs := splitDocuments([]byte(data))

	var kinds []string
	for _, doc := range docs {
		kinds = append(kinds, strings.TrimSpace(strings.TrimPrefix(lastLine(string(doc)), "kind:")))
	}
	if strings.Join(kinds, " ") != "Pod Service Job" {
		t.Errorf("got documents of kinds %v, want Pod Service Job", kinds)
	}
}

// lastLine returns the last non-empty line of text.
func lastLine(text string) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	return lines[len(lines)-1]
}

// newTestCLI returns a cli connected to an API server in memory.
func newTestCLI(t *testing.T) (*cli, *store.Store) {
	t.Helper()

	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	t.Cleanup(ts.Close)
	return &cli{client: client.New(ts.URL), namespace: api.DefaultNamespace}, st
}

// TestApplyExamples applies the whole examples folder: every file must be
// accepted, and applying it all a second time must update, not fail.
func TestApplyExamples(t *testing.T) {
	c, st := newTestCLI(t)

	for range 2 {
		if err := c.applyPath("../../examples"); err != nil {
			t.Fatalf("apply examples: %v", err)
		}
	}

	d := st.ListDeployments(api.DefaultNamespace)
	if len(d) != 1 || d[0].Template.Containers[0].Ports[0].ContainerPort != 80 {
		t.Errorf("got deployments %+v, want web with container port 80", d)
	}
	svcs := st.ListServices(api.DefaultNamespace)
	if len(svcs) != 1 || svcs[0].Ports[0].Port != 8081 || svcs[0].Ports[0].Target() != 80 {
		t.Errorf("got services %+v, want web on 8081 -> 80", svcs)
	}
	secret, ok := st.Secrets.Get(api.DefaultNamespace, "app-secret")
	if !ok || string(secret.Data["password"]) != "correct-horse-battery-staple" {
		t.Errorf("got secret %+v (found: %t), want stringData turned into data", secret, ok)
	}
	if cj, ok := st.CronJobs.Get(api.DefaultNamespace, "clock"); !ok || cj.JobTemplate.Spec.Template.Containers[0].Image != "busybox:1.36" {
		t.Errorf("got cronjob %+v (found: %t), want clock running busybox", cj, ok)
	}
}

func TestApplyMultipleDocumentsAndNamespaces(t *testing.T) {
	c, st := newTestCLI(t)

	path := filepath.Join(t.TempDir(), "app.yaml")
	os.WriteFile(path, []byte(`apiVersion: v1
kind: Namespace
metadata:
  name: shop
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: settings
  namespace: shop
data:
  mode: fast
`), 0o644)

	if err := c.applyPath(path); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, ok := st.ConfigMaps.Get("shop", "settings"); !ok {
		t.Error("the configmap wasn't created in the namespace from its metadata")
	}

	// A wrong apiVersion is refused.
	os.WriteFile(path, []byte("apiVersion: apps/v1\nkind: ConfigMap\nmetadata:\n  name: x\n"), 0o644)
	if err := c.applyPath(path); err == nil {
		t.Error("a ConfigMap with apiVersion apps/v1 was accepted")
	}
}
