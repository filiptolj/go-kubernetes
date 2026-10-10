package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/apiserver"
	"github.com/filiptolj/go-kubernetes/pkg/client"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// ns is the namespace the tests use.
const ns = api.DefaultNamespace

// newTestCluster starts an API server in memory and returns its store, for
// setting things up and checking results, and a client connected to it.
func newTestCluster(t *testing.T) (*store.Store, *client.Client) {
	t.Helper()

	st := store.New()
	ts := httptest.NewServer(apiserver.NewHandler(st))
	t.Cleanup(ts.Close)

	return st, client.New(ts.URL)
}

// meta returns the metadata of an object in ns.
func meta(name string) api.ObjectMeta {
	return api.ObjectMeta{Name: name, Namespace: ns}
}

// tmpl returns a pod template with labels, running the given images.
func tmpl(labels api.Labels, images ...string) api.PodTemplateSpec {
	t := api.PodTemplateSpec{ObjectMeta: api.ObjectMeta{Labels: labels}}
	for i, image := range images {
		t.Containers = append(t.Containers, api.Container{Name: fmt.Sprintf("c%d", i), Image: image})
	}
	return t
}

// replicaSet returns a ReplicaSet in namespace.
func replicaSet(namespace, name string, replicas int, t api.PodTemplateSpec) api.ReplicaSet {
	return api.ReplicaSet{
		ObjectMeta:     api.ObjectMeta{Name: name, Namespace: namespace},
		ReplicaSetSpec: api.ReplicaSetSpec{Replicas: replicas, Template: t},
	}
}

// ownedPod returns a pod in ns, in the given phase, controlled by an object
// of kind.
func ownedPod(name, kind, owner string, phase api.PodPhase) api.Pod {
	pod := api.Pod{ObjectMeta: meta(name), PodStatus: api.PodStatus{Phase: phase}}
	pod.SetOwner(kind, owner, "")
	return pod
}

// alivePods returns the live pods that belong to a ReplicaSet.
func alivePods(st *store.Store, owner string) []api.Pod {
	var pods []api.Pod
	for _, pod := range st.ListPods("") {
		if pod.OwnerName() == owner && isAlive(pod) {
			pods = append(pods, pod)
		}
	}
	return pods
}

func TestReplicaSetController(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{Client: c}

	st.CreateReplicaSet(replicaSet(ns, "web", 3, tmpl(nil, "nginx")))

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
	(&GarbageCollector{Client: c}).collect()
	if got := len(st.ListPods("")); got != 0 {
		t.Errorf("after deleting the replicaset: got %d pods, want 0", got)
	}
}

func TestReplicaSetControllerRemovesPendingPodsFirst(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{Client: c}

	st.CreateReplicaSet(replicaSet(ns, "web", 1, tmpl(nil, "nginx")))
	st.CreatePod(ownedPod("web-running", "ReplicaSet", "web", api.PodRunning))
	st.CreatePod(ownedPod("web-pending", "ReplicaSet", "web", api.PodPending))

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

			st.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-1"}, NodeStatus: api.NodeStatus{Ready: true}})
			st.CreatePod(api.Pod{ObjectMeta: meta("nginx"), PodSpec: api.PodSpec{NodeName: "node-1"}, PodStatus: api.PodStatus{Phase: api.PodRunning}})

			// A node created with apply has no heartbeat and must be left alone.
			st.CreateNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "static"}, NodeStatus: api.NodeStatus{Ready: true}})

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

	flaky := replicaSet(ns, "flaky", 1, tmpl(nil, "busybox"))
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
		rc.recordFailure(flaky, api.Pod{ObjectMeta: meta("x")}) // never started, so it counts as a crash

		got := rc.backoff[api.Key(ns, "flaky")].until.Sub(before).Round(time.Minute)
		if got != wantDelay {
			t.Errorf("crash %d in a row: waiting %s, want %s", i+2, got, wantDelay)
		}
	}

	// A pod that ran for a long time before failing resets the count.
	start := time.Now().Add(-2 * time.Minute)
	rc.recordFailure(flaky, api.Pod{ObjectMeta: meta("y"), PodStatus: api.PodStatus{StartedAt: start, FinishedAt: time.Now()}})
	if f := rc.backoff[api.Key(ns, "flaky")].failures; f != 1 {
		t.Errorf("after a pod that ran 2 minutes failed: %d failures in a row, want 1", f)
	}
}

func TestReplicaSetCopiesLabels(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{Client: c}

	st.CreateReplicaSet(replicaSet(ns, "web", 1, tmpl(api.Labels{"app": "web"}, "nginx")))
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

	deployment := func(image string) api.Deployment {
		return api.Deployment{
			ObjectMeta:     meta("web"),
			DeploymentSpec: api.DeploymentSpec{Replicas: 3, Template: tmpl(api.Labels{"app": "web"}, image)},
		}
	}

	// step runs both controllers once and lets new pods start.
	step := func() {
		dc.reconcileAll()
		rc.reconcileAll()
		runPods(st)
	}

	st.CreateDeployment(deployment("nginx:1.27"))
	step()

	sets := st.ListReplicaSets("")
	if len(sets) != 1 || sets[0].Replicas != 3 || !sets[0].OwnedBy("Deployment", "web") {
		t.Fatalf("after creating the deployment: got replicasets %+v, want one with 3 replicas", sets)
	}
	oldName := sets[0].Name

	// Change the image: the rolling update begins.
	st.UpdateDeployment(deployment("nginx:1.28"))

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

	// The old version is kept, scaled to 0, as revision 1.
	replicas := func() map[string]string {
		got := make(map[string]string)
		for _, rs := range st.ListReplicaSets("") {
			got[rs.Name] = fmt.Sprintf("%d replicas, revision %s", rs.Replicas, rs.Annotations[api.RevisionAnnotation])
		}
		return got
	}
	newName := "web-" + templateHash(deployment("nginx:1.28").Template)
	want := map[string]string{oldName: "0 replicas, revision 1", newName: "3 replicas, revision 2"}
	if got := replicas(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("after the update: got replicasets %v, want %v", got, want)
	}
	for _, pod := range alivePods(st, newName) {
		if pod.Containers[0].Image != "nginx:1.28" {
			t.Errorf("pod %s runs %s, want nginx:1.28", pod.Name, pod.Containers[0].Image)
		}
	}

	// Going back to the old template (what `rollout undo` does) makes the
	// old ReplicaSet current again, as the newest revision.
	d := st.ListDeployments(ns)[0]
	d.Template = deployment("nginx:1.27").Template
	st.UpdateDeployment(d)
	for range 20 {
		step()
	}
	want = map[string]string{oldName: "3 replicas, revision 3", newName: "0 replicas, revision 2"}
	if got := replicas(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("after going back: got replicasets %v, want %v", got, want)
	}

	// Deleting the deployment removes its replicaset, and then its pods.
	st.DeleteDeployment(ns, "web")
	gc := &GarbageCollector{Client: c}
	gc.collect() // the ReplicaSet
	gc.collect() // then its pods
	if n := len(st.ListReplicaSets("")); n != 0 {
		t.Errorf("after deleting the deployment: %d replicasets left, want 0", n)
	}
	if n := len(st.ListPods("")); n != 0 {
		t.Errorf("after deleting the deployment: %d pods left, want 0", n)
	}
}

func TestTemplateHash(t *testing.T) {
	a := tmpl(api.Labels{"app": "web"}, "nginx:1.27")
	b := tmpl(api.Labels{"app": "web"}, "nginx:1.27")
	changed := tmpl(api.Labels{"app": "web"}, "nginx:1.28")

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
	st.CreateNamespace(api.Namespace{ObjectMeta: api.ObjectMeta{Name: "dev"}})

	st.CreateReplicaSet(replicaSet("default", "web", 3, tmpl(nil, "nginx")))
	st.CreateReplicaSet(replicaSet("dev", "web", 1, tmpl(nil, "nginx")))

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

// TestControllersWithInformers runs the ReplicaSet and Deployment controllers
// the way the controller-manager does: reading from informers, and woken up
// by their changes. Their caches lag behind the API server, so without
// expectations the controllers would create too many pods and ReplicaSets.
func TestControllersWithInformers(t *testing.T) {
	// Watches deliver every event 100ms late, so the informers really do lag.
	st := store.New()
	ts := httptest.NewServer(slowWatches(apiserver.NewHandler(st), 100*time.Millisecond))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // before ts.Close, which waits for the watches to end

	informers := NewInformers(c)
	go informers.Run(ctx)
	rc := &ReplicaSetController{Client: c, Informers: informers, Resync: time.Hour}
	dc := &DeploymentController{Client: c, Informers: informers, Every: time.Hour}
	go rc.Run(ctx)
	go dc.Run(ctx)

	// Count every pod ever created, not only the ones still there.
	created := 0
	events, stop := st.Watch("pods", "")
	defer stop()

	st.CreateDeployment(api.Deployment{
		ObjectMeta:     meta("web"),
		DeploymentSpec: api.DeploymentSpec{Replicas: 5, Template: tmpl(api.Labels{"app": "web"}, "nginx")},
	})

	deadline := time.After(2 * time.Second)
	for done := false; !done; {
		select {
		case event := <-events:
			if event.Type == api.EventAdded {
				created++
			}
		case <-deadline:
			done = true
		}
	}

	if created != 5 {
		t.Errorf("created %d pods, want exactly 5", created)
	}
	if sets := st.ListReplicaSets(ns); len(sets) != 1 {
		t.Errorf("got %d replicasets, want 1", len(sets))
	}
}

// slowWatches wraps an API server so that watches deliver each event late.
func slowWatches(h http.Handler, delay time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" {
			w = &slowWriter{ResponseWriter: w, delay: delay}
		}
		h.ServeHTTP(w, r)
	})
}

type slowWriter struct {
	http.ResponseWriter
	delay time.Duration
}

func (w *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	return w.ResponseWriter.Write(p)
}

// Unwrap lets http.NewResponseController reach the real writer, to flush it.
func (w *slowWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func TestGarbageCollector(t *testing.T) {
	st, c := newTestCluster(t)
	gc := &GarbageCollector{Client: c}

	rs, _ := st.CreateReplicaSet(replicaSet(ns, "web", 1, tmpl(nil, "nginx")))
	owned := func(name, uid string) api.Pod {
		pod := api.Pod{ObjectMeta: meta(name)}
		pod.SetOwner("ReplicaSet", "web", uid)
		return pod
	}
	st.CreatePod(owned("mine", rs.UID))
	st.CreatePod(owned("older-web", "an-old-uid")) // from an earlier ReplicaSet called web
	st.CreatePod(owned("unknown-uid", ""))         // no UID: the name decides
	st.CreatePod(api.Pod{ObjectMeta: meta("standalone")})
	gone := api.Pod{ObjectMeta: meta("orphan")}
	gone.SetOwner("ReplicaSet", "deleted", "x")
	st.CreatePod(gone)
	strange := api.Pod{ObjectMeta: meta("strange-owner")}
	strange.SetOwner("Banana", "b", "y") // a kind it doesn't know: leave it alone
	st.CreatePod(strange)

	gc.collect()

	var left []string
	for _, pod := range st.ListPods(ns) {
		left = append(left, pod.Name)
	}
	want := []string{"mine", "standalone", "strange-owner", "unknown-uid"}
	if fmt.Sprint(left) != fmt.Sprint(want) {
		t.Errorf("pods left: got %v, want %v", left, want)
	}
}

func TestDeploymentKeepsLimitedHistory(t *testing.T) {
	st, c := newTestCluster(t)
	dc := &DeploymentController{Client: c}
	rc := &ReplicaSetController{Client: c}

	limit := 2
	d := api.Deployment{ObjectMeta: meta("web"), DeploymentSpec: api.DeploymentSpec{Replicas: 1, RevisionHistoryLimit: &limit}}
	st.CreateDeployment(d)
	for _, image := range []string{"v1", "v2", "v3", "v4", "v5"} {
		d := st.ListDeployments(ns)[0]
		d.Template = tmpl(api.Labels{"app": "web"}, image)
		st.UpdateDeployment(d)
		for range 10 {
			dc.reconcileAll()
			rc.reconcileAll()
			runPods(st)
		}
	}

	// v5 runs; of the old versions, only the 2 newest are kept.
	var revisions []string
	for _, rs := range st.ListReplicaSets(ns) {
		revisions = append(revisions, rs.Annotations[api.RevisionAnnotation])
	}
	slices.Sort(revisions)
	if fmt.Sprint(revisions) != "[3 4 5]" {
		t.Errorf("got revisions %v, want [3 4 5]", revisions)
	}
}

func TestTerminatingPodIsReplacedAtOnce(t *testing.T) {
	st, c := newTestCluster(t)
	rc := &ReplicaSetController{Client: c}
	st.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-1"}, NodeStatus: api.NodeStatus{Ready: true}})
	st.CreateReplicaSet(replicaSet(ns, "web", 1, tmpl(nil, "nginx")))

	rc.reconcileAll()
	first := alivePods(st, "web")[0].Name
	st.BindPod(ns, first, "node-1")
	runPods(st)

	// Deleted while running on a node: it terminates, and meanwhile a new
	// pod takes its place.
	st.DeletePodGracefully(ns, first, 30)
	rc.reconcileAll()
	rc.reconcileAll()

	pods := alivePods(st, "web")
	if len(pods) != 1 || pods[0].Name == first {
		t.Errorf("got live pods %v, want one new pod", pods)
	}
	if old, ok := st.GetPod(ns, first); !ok || !old.Terminating() {
		t.Errorf("the old pod should still be there, terminating: %+v", old.ObjectMeta)
	}
}
