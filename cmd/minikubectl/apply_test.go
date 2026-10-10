package main

import (
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
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

	deployments := st.ListDeployments(api.DefaultNamespace)
	i := slices.IndexFunc(deployments, func(d api.Deployment) bool { return d.Name == "web" })
	if i < 0 || deployments[i].Template.Containers[0].Ports[0].ContainerPort != 80 {
		t.Errorf("got deployments %+v, want web with container port 80", deployments)
	}
	services := st.ListServices(api.DefaultNamespace)
	i = slices.IndexFunc(services, func(s api.Service) bool { return s.Name == "web" })
	if i < 0 || services[i].Ports[0].Port != 8081 || services[i].Ports[0].Target() != 80 {
		t.Errorf("got services %+v, want web on 8081 -> 80", services)
	}
	if _, ok := st.Autoscalers.Get(api.DefaultNamespace, "php-apache"); !ok {
		t.Error("the autoscaler from autoscaling.yaml wasn't created")
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

func TestListWithSelectorAndName(t *testing.T) {
	c, st := newTestCLI(t)
	for name, app := range map[string]string{"web-1": "web", "web-2": "web", "db-1": "db"} {
		st.CreatePod(api.Pod{ObjectMeta: api.ObjectMeta{Name: name, Namespace: "default", Labels: api.Labels{"app": app}}})
	}

	c.selector = "app=web"
	pods, err := list[api.Pod](c, "pods")
	if err != nil || len(pods) != 2 {
		t.Errorf("-l app=web: got %d pods, %v; want 2", len(pods), err)
	}

	c.selector, c.name = "", "db-1"
	pods, err = list[api.Pod](c, "pods")
	if err != nil || len(pods) != 1 || pods[0].Name != "db-1" {
		t.Errorf("name db-1: got %v, %v", pods, err)
	}

	c.name = "nope"
	if _, err := list[api.Pod](c, "pods"); err == nil {
		t.Error("an unknown name: want an error")
	}

	// Nodes don't live in a namespace: -n must not get in the way.
	st.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-1"}})
	c.name = ""
	if nodes, err := list[api.Node](c, "nodes"); err != nil || len(nodes) != 1 {
		t.Errorf("nodes: got %v, %v", nodes, err)
	}
}

func TestGetCommandArguments(t *testing.T) {
	c, _ := newTestCLI(t)
	for _, args := range [][]string{
		{"pods", "-l"},
		{"pods", "-l", "=web"},
		{"pods", "-x"},
		{"pods", "a", "b"},
		{"deployments", "-w"},
		{"pods", "-o", "xml"},
	} {
		c.name, c.selector = "", ""
		if err := c.getCommand(args); err == nil {
			t.Errorf("get %v: want an error", args)
		}
	}
}

func TestRolloutUndo(t *testing.T) {
	c, st := newTestCLI(t)

	template := func(image string) api.PodTemplateSpec {
		return api.PodTemplateSpec{
			ObjectMeta: api.ObjectMeta{Labels: api.Labels{"app": "web"}},
			PodSpec:    api.PodSpec{Containers: []api.Container{{Name: "c", Image: image}}},
		}
	}
	d, _ := st.CreateDeployment(api.Deployment{
		ObjectMeta:     api.ObjectMeta{Name: "web", Namespace: "default"},
		DeploymentSpec: api.DeploymentSpec{Replicas: 1, Template: template("v3")},
	})
	for rev, image := range map[int]string{1: "v1", 2: "v2", 3: "v3"} {
		rs := api.ReplicaSet{
			ObjectMeta: api.ObjectMeta{Name: "web-" + image, Namespace: "default",
				Annotations: map[string]string{api.RevisionAnnotation: fmt.Sprint(rev)}},
			ReplicaSetSpec: api.ReplicaSetSpec{Template: template(image)},
		}
		rs.SetOwner("Deployment", "web", d.UID)
		st.CreateReplicaSet(rs)
	}

	image := func() string { return st.ListDeployments("default")[0].Template.Containers[0].Image }

	if err := c.rolloutUndo("web", 0); err != nil || image() != "v2" {
		t.Errorf("undo: got image %s, %v; want v2, the revision before v3", image(), err)
	}
	if err := c.rolloutUndo("web", 1); err != nil || image() != "v1" {
		t.Errorf("undo to revision 1: got image %s, %v; want v1", image(), err)
	}
	if err := c.rolloutUndo("web", 9); err == nil {
		t.Error("undo to a revision that doesn't exist: want an error")
	}
}
