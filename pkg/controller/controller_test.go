package controller

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// newTestCluster starts an API server in memory and returns its store, for
// setting things up and checking results, and a client connected to it.
// ns is the namespace the tests use.
const ns = api.DefaultNamespace

func newTestCluster(t *testing.T) (*store.Store, *client.Client) {
	t.Helper()

	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	t.Cleanup(ts.Close)

	return st, client.New(ts.URL)
}

// alivePods returns the live pods that belong to a ReplicaSet.
func alivePods(st *store.Store, owner string) []api.Pod {
	var pods []api.Pod
	for _, pod := range st.ListPods("") {
		if pod.Owner == owner && isAlive(pod) {
			pods = append(pods, pod)
		}
	}
	return pods
}

func TestReplicaSetController(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{Client: c}

	st.CreateReplicaSet(api.ReplicaSet{
		Name:      "web",
		Namespace: ns,
		Replicas:  3,
		Template:  api.PodTemplate{Containers: []api.Container{{Name: "nginx", Image: "nginx"}}},
	})

	// Too few pods: it creates them.
	rc.reconcileAll()
	pods := alivePods(st, "web")
	if len(pods) != 3 {
		t.Fatalf("after creating the replicaset: got %d pods, want 3", len(pods))
	}

	// Running it again changes nothing: the state already matches.
	rc.reconcileAll()
	if got := len(st.ListPods("")); got != 3 {
		t.Fatalf("after a second reconcile: got %d pods, want still 3", got)
	}

	// A pod fails: it is removed and replaced.
	failed := pods[0].Name
	st.SetPodStatus(ns, failed, api.PodStatus{Phase: api.PodFailed})
	rc.reconcileAll()
	if _, ok := st.GetPod(ns, failed); ok {
		t.Errorf("failed pod %q still exists, want it deleted", failed)
	}
	if got := len(alivePods(st, "web")); got != 3 {
		t.Errorf("after a pod failed: got %d live pods, want 3", got)
	}

	// Scale down: extra pods are removed.
	st.ScaleReplicaSet(ns, "web", 1)
	rc.reconcileAll()
	if got := len(alivePods(st, "web")); got != 1 {
		t.Errorf("after scaling to 1: got %d live pods, want 1", got)
	}

	// The ReplicaSet is deleted: its pods are garbage collected.
	st.DeleteReplicaSet(ns, "web")
	rc.reconcileAll()
	if got := len(st.ListPods("")); got != 0 {
		t.Errorf("after deleting the replicaset: got %d pods, want 0", got)
	}
}

func TestReplicaSetControllerRemovesPendingPodsFirst(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{Client: c}

	st.CreateReplicaSet(api.ReplicaSet{Namespace: ns, Name: "web", Replicas: 1})
	st.CreatePod(api.Pod{Namespace: ns, Name: "web-running", Owner: "web", Phase: api.PodRunning})
	st.CreatePod(api.Pod{Namespace: ns, Name: "web-pending", Owner: "web", Phase: api.PodPending})

	rc.reconcileAll()

	pods := alivePods(st, "web")
	if len(pods) != 1 || pods[0].Name != "web-running" {
		t.Errorf("got pods %v, want only web-running", pods)
	}
}

func TestNodeController(t *testing.T) {
	tests := []struct {
		name          string
		timeout       time.Duration
		wantNodeReady bool
		wantPodPhase  api.PodPhase
	}{
		{name: "recent heartbeat", timeout: time.Hour, wantNodeReady: true, wantPodPhase: api.PodRunning},
		{name: "heartbeat too old", timeout: time.Nanosecond, wantNodeReady: false, wantPodPhase: api.PodFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, c := newTestCluster(t)
			nc := &NodeController{Client: c, Timeout: tt.timeout}

			st.PutNode(api.Node{Name: "node-1", Ready: true})
			st.CreatePod(api.Pod{Namespace: ns, Name: "nginx", NodeName: "node-1", Phase: api.PodRunning})

			// A node created with apply has no heartbeat and must be left alone.
			st.CreateNode(api.Node{Name: "static", Ready: true})

			time.Sleep(time.Millisecond) // let the heartbeat age past a 1ns timeout
			nc.reconcile()

			nodes := st.ListNodes()
			if nodes[0].Name != "node-1" || nodes[0].Ready != tt.wantNodeReady {
				t.Errorf("node-1: got Ready %t, want %t", nodes[0].Ready, tt.wantNodeReady)
			}
			if !nodes[1].Ready {
				t.Errorf("static node was marked NotReady; it has no kubelet and should be left alone")
			}

			pod, _ := st.GetPod(ns, "nginx")
			if pod.Phase != tt.wantPodPhase {
				t.Errorf("pod: got phase %s, want %s", pod.Phase, tt.wantPodPhase)
			}
		})
	}
}

func TestReplicaSetBackoff(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{
		Client:       c,
		BackoffBase:  time.Hour, // long enough that the test never waits it out
		BackoffMax:   4 * time.Hour,
		HealthyAfter: time.Minute,
	}

	flaky := api.ReplicaSet{Namespace: ns, Name: "flaky", Replicas: 1, Template: api.PodTemplate{
		Containers: []api.Container{{Name: "main", Image: "busybox"}},
	}}
	st.CreateReplicaSet(flaky)
	rc.reconcileAll()

	pod := alivePods(st, "flaky")[0]
	st.SetPodStatus(ns, pod.Name, api.PodStatus{Phase: api.PodRunning})
	st.SetPodStatus(ns, pod.Name, api.PodStatus{Phase: api.PodFailed})
	rc.reconcileAll()

	if got := len(st.ListPods("")); got != 0 {
		t.Fatalf("right after a crash: got %d pods, want 0 (backing off)", got)
	}

	// That was crash 1. The wait doubles with every crash in a row, up to
	// the maximum.
	wantDelays := []time.Duration{2 * time.Hour, 4 * time.Hour, 4 * time.Hour}
	for i, wantDelay := range wantDelays {
		before := time.Now()
		rc.recordFailure(flaky, api.Pod{Namespace: ns, Name: "x"}) // never started, so it counts as a crash

		got := rc.backoff[api.Key(ns, "flaky")].until.Sub(before).Round(time.Minute)
		if got != wantDelay {
			t.Errorf("crash %d in a row: waiting %s, want %s", i+2, got, wantDelay)
		}
	}

	// A pod that ran for a long time before failing resets the count.
	start := time.Now().Add(-2 * time.Minute)
	rc.recordFailure(flaky, api.Pod{Namespace: ns, Name: "y", StartedAt: start, FinishedAt: time.Now()})
	if f := rc.backoff[api.Key(ns, "flaky")].failures; f != 1 {
		t.Errorf("after a pod that ran 2 minutes failed: %d failures in a row, want 1", f)
	}
}

func TestReplicaSetCopiesLabels(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{Client: c}

	st.CreateReplicaSet(api.ReplicaSet{Namespace: ns, Name: "web", Replicas: 1, Template: api.PodTemplate{
		Labels:     api.Labels{"app": "web"},
		Containers: []api.Container{{Name: "nginx", Image: "nginx"}},
	}})
	rc.reconcileAll()

	pod := alivePods(st, "web")[0]
	if pod.Labels["app"] != "web" {
		t.Errorf("pod labels: got %v, want app=web", pod.Labels)
	}
}

// runPods marks every pending pod Running and ready, playing the part of the kubelets.
func runPods(st *store.Store) {
	for _, pod := range st.ListPods("") {
		if pod.Phase == api.PodPending || pod.Phase == "" {
			st.SetPodStatus(ns, pod.Name, api.PodStatus{Phase: api.PodRunning, Ready: true})
		}
	}
}

func TestDeploymentRollingUpdate(t *testing.T) {
	st, c := newTestCluster(t)
	dc := &DeploymentController{Client: c}
	rc := &ReplicaSetController{Client: c}

	template := func(image string) api.PodTemplate {
		return api.PodTemplate{
			Labels:     api.Labels{"app": "web"},
			Containers: []api.Container{{Name: "nginx", Image: image}},
		}
	}

	// step runs both controllers once and lets new pods start.
	step := func() {
		dc.reconcileAll()
		rc.reconcileAll()
		runPods(st)
	}

	st.CreateDeployment(api.Deployment{Namespace: ns, Name: "web", Replicas: 3, Template: template("nginx:1.27")})
	step()

	sets := st.ListReplicaSets("")
	if len(sets) != 1 || sets[0].Replicas != 3 || sets[0].Owner != "web" {
		t.Fatalf("after creating the deployment: got replicasets %+v, want one with 3 replicas", sets)
	}
	oldName := sets[0].Name

	// Change the image: the rolling update begins.
	st.UpdateDeployment(api.Deployment{Namespace: ns, Name: "web", Replicas: 3, Template: template("nginx:1.28")})

	for i := range 20 {
		step()

		// The promise of a rolling update: never more than 4 pods (3+1),
		// never fewer than 3 running and ready.
		running, alive := 0, 0
		for _, pod := range st.ListPods("") {
			if isAlive(pod) {
				alive++
			}
			if pod.Phase == api.PodRunning && pod.Ready {
				running++
			}
		}
		if alive > 4 || running < 3 {
			t.Fatalf("step %d: %d pods alive and %d running; want at most 4 and at least 3", i, alive, running)
		}
	}

	sets = st.ListReplicaSets("")
	if len(sets) != 1 || sets[0].Name == oldName || sets[0].Replicas != 3 {
		t.Fatalf("after the update: got replicasets %+v, want only the new one with 3 replicas", sets)
	}
	for _, pod := range alivePods(st, sets[0].Name) {
		if pod.Containers[0].Image != "nginx:1.28" {
			t.Errorf("pod %s runs %s, want nginx:1.28", pod.Name, pod.Containers[0].Image)
		}
	}

	// Deleting the deployment removes its replicaset, and then its pods.
	st.DeleteDeployment(ns, "web")
	step()
	step()
	if n := len(st.ListReplicaSets("")); n != 0 {
		t.Errorf("after deleting the deployment: %d replicasets left, want 0", n)
	}
	if n := len(st.ListPods("")); n != 0 {
		t.Errorf("after deleting the deployment: %d pods left, want 0", n)
	}
}

func TestTemplateHash(t *testing.T) {
	a := api.PodTemplate{Labels: api.Labels{"app": "web"}, Containers: []api.Container{{Name: "c", Image: "nginx:1.27"}}}
	b := api.PodTemplate{Labels: api.Labels{"app": "web"}, Containers: []api.Container{{Name: "c", Image: "nginx:1.27"}}}
	changed := api.PodTemplate{Labels: api.Labels{"app": "web"}, Containers: []api.Container{{Name: "c", Image: "nginx:1.28"}}}

	if templateHash(a) != templateHash(b) {
		t.Error("identical templates got different hashes")
	}
	if templateHash(a) == templateHash(changed) {
		t.Error("different templates got the same hash")
	}
}

// TestNamespacesKeepTheirPods checks that two ReplicaSets with the same name,
// in different namespaces, each manage only their own pods.
func TestNamespacesKeepTheirPods(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{Client: c}
	st.CreateNamespace(api.Namespace{Name: "dev"})

	template := api.PodTemplate{Containers: []api.Container{{Name: "nginx", Image: "nginx"}}}
	st.CreateReplicaSet(api.ReplicaSet{Namespace: "default", Name: "web", Replicas: 3, Template: template})
	st.CreateReplicaSet(api.ReplicaSet{Namespace: "dev", Name: "web", Replicas: 1, Template: template})

	rc.reconcileAll()
	rc.reconcileAll()

	if n := len(st.ListPods("default")); n != 3 {
		t.Errorf("default: got %d pods, want 3", n)
	}
	if n := len(st.ListPods("dev")); n != 1 {
		t.Errorf("dev: got %d pods, want 1", n)
	}
	for _, pod := range st.ListPods("dev") {
		if pod.Namespace != "dev" {
			t.Errorf("pod %s of dev/web was created in namespace %q", pod.Name, pod.Namespace)
		}
	}
}
