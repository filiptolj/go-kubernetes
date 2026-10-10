package controller

import (
	"slices"
	"testing"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/store"
)

// busybox is a pod template for the tests.
var busybox = api.PodTemplateSpec{
	ObjectMeta: api.ObjectMeta{Labels: api.Labels{"app": "test"}},
	PodSpec: api.PodSpec{
		Containers:    []api.Container{{Name: "main", Image: "busybox"}},
		RestartPolicy: api.RestartAlways,
	},
}

// setPhase changes the phase of every pod in pods whose name is in names.
func setPhase(st *store.Store, phase api.PodPhase, names ...string) {
	for _, name := range names {
		st.SetPodStatus(ns, name, api.PodStatus{Phase: phase, Ready: phase == api.PodRunning})
	}
}

// podNames returns the names of the alive pods owned by kind/owner.
func podNames(st *store.Store, kind, owner string) []string {
	var names []string
	for _, pod := range st.ListPods(ns) {
		if pod.OwnedBy(kind, owner) && isAlive(pod) {
			names = append(names, pod.Name)
		}
	}
	return names
}

// intPtr returns a pointer to n, for fields like BackoffLimit.
func intPtr(n int) *int { return &n }

func TestJobRunsUntilEnoughSucceed(t *testing.T) {
	st, c := newTestCluster(t)
	jc := &JobController{Client: c}

	template := busybox
	template.RestartPolicy = api.RestartNever
	st.Jobs.Create(api.Job{
		ObjectMeta: meta("report"),
		JobSpec:    api.JobSpec{Completions: 3, Parallelism: 2, BackoffLimit: intPtr(6), Template: template},
	})

	jc.reconcileAll()
	running := podNames(st, "Job", "report")
	if len(running) != 2 {
		t.Fatalf("got %d pods, want 2: parallelism is 2", len(running))
	}

	// Both succeed: one more is still needed.
	setPhase(st, api.PodSucceeded, running...)
	jc.reconcileAll()
	running = podNames(st, "Job", "report")
	if len(running) != 1 {
		t.Fatalf("got %d running pods, want 1 for the last completion", len(running))
	}

	setPhase(st, api.PodSucceeded, running...)
	jc.reconcileAll()

	job, _ := st.Jobs.Get(ns, "report")
	if job.Status.Condition != api.JobComplete || job.Status.Succeeded != 3 {
		t.Errorf("got status %+v, want Complete with 3 succeeded", job.Status)
	}

	// A finished Job keeps its finished pods, for their logs, but starts no more.
	jc.reconcileAll()
	if n := len(st.ListPods(ns)); n != 3 {
		t.Errorf("got %d pods after completion, want the 3 finished ones", n)
	}
}

func TestJobGivesUpAfterTheBackoffLimit(t *testing.T) {
	st, c := newTestCluster(t)
	jc := &JobController{Client: c}

	template := busybox
	template.RestartPolicy = api.RestartNever
	st.Jobs.Create(api.Job{
		ObjectMeta: meta("flaky"),
		JobSpec:    api.JobSpec{Completions: 1, Parallelism: 1, BackoffLimit: intPtr(1), Template: template},
	})

	for range 2 {
		jc.reconcileAll()
		setPhase(st, api.PodFailed, podNames(st, "Job", "flaky")...)
	}
	jc.reconcileAll()

	job, _ := st.Jobs.Get(ns, "flaky")
	if job.Status.Condition != api.JobFailed || job.Status.Failed != 2 {
		t.Errorf("got status %+v, want Failed after 2 failures", job.Status)
	}
	if running := podNames(st, "Job", "flaky"); len(running) != 0 {
		t.Errorf("pods %v still run after the job failed", running)
	}
}

func TestCronJob(t *testing.T) {
	st, c := newTestCluster(t)
	clock := time.Date(2026, 10, 8, 10, 2, 0, 0, time.UTC)
	cc := &CronJobController{Client: c, Now: func() time.Time { return clock }}

	// The test writes straight into the store, so it must give what the API
	// server would have filled in: Jobs may not use restartPolicy Always.
	template := busybox
	template.RestartPolicy = api.RestartNever
	st.CronJobs.Create(api.CronJob{
		ObjectMeta: meta("backup"),
		CronJobSpec: api.CronJobSpec{
			Schedule:    "*/5 * * * *",
			JobTemplate: api.JobTemplateSpec{Spec: api.JobSpec{Completions: 1, Parallelism: 1, Template: template}},
		},
	})

	jobNames := func() []string {
		var names []string
		for _, job := range st.Jobs.List(ns) {
			names = append(names, job.Name)
		}
		return names
	}

	cc.reconcileAll() // 10:02: starts counting from now
	clock = clock.Add(2 * time.Minute)
	cc.reconcileAll() // 10:04: nothing due yet
	if names := jobNames(); len(names) != 0 {
		t.Fatalf("at 10:04 got jobs %v, want none", names)
	}

	clock = clock.Add(2 * time.Minute)
	cc.reconcileAll() // 10:06: 10:05 was due
	cc.reconcileAll() // checking again must not create a second one
	names := jobNames()
	// The name ends with the due time in minutes since 1970: 10:05 is 29857565.
	if len(names) != 1 || names[0] != "backup-29857565" {
		t.Fatalf("at 10:06 got jobs %v, want exactly one, named after 10:05", names)
	}

	// Suspended: 10:10 passes without a job, and isn't run later either.
	cj, _ := st.CronJobs.Get(ns, "backup")
	cj.Suspend = true
	st.CronJobs.Update(cj)
	clock = clock.Add(5 * time.Minute)
	cc.reconcileAll() // 10:11

	// Read it again: the controller has saved a newer status meanwhile. (The
	// API server would keep the stored status on an update anyway; the test
	// writes to the store directly, which doesn't.)
	cj, _ = st.CronJobs.Get(ns, "backup")
	cj.Suspend = false
	st.CronJobs.Update(cj)
	clock = clock.Add(time.Minute)
	cc.reconcileAll() // 10:12
	if n := len(jobNames()); n != 1 {
		t.Errorf("after the suspended time: got %d jobs, want still 1", n)
	}

	// Deleting the CronJob deletes its Jobs.
	st.CronJobs.Delete(ns, "backup")
	(&GarbageCollector{Client: c}).collect()
	if n := len(jobNames()); n != 0 {
		t.Errorf("after deleting the cronjob: got %d jobs, want 0", n)
	}
}

func TestCronJobKeepsAFewFinishedJobs(t *testing.T) {
	st, c := newTestCluster(t)
	cc := &CronJobController{Client: c}

	st.CronJobs.Create(api.CronJob{
		ObjectMeta: meta("backup"),
		CronJobSpec: api.CronJobSpec{
			Schedule:    "0 0 1 1 *", // once a year, so it won't run during the test
			JobTemplate: api.JobTemplateSpec{Spec: api.JobSpec{Template: busybox}},
		},
		Status: api.CronJobStatus{LastScheduleTime: time.Now()},
	})
	for i, name := range []string{"j1", "j2", "j3", "j4", "j5"} {
		job := api.Job{
			ObjectMeta: meta(name),
			Status: api.JobStatus{
				Condition:      api.JobComplete,
				CompletionTime: time.Now().Add(time.Duration(i) * time.Minute), // j5 is the newest
			},
		}
		job.SetOwner("CronJob", "backup", "")
		st.Jobs.Create(job)
	}

	cc.reconcileAll()

	var left []string
	for _, job := range st.Jobs.List(ns) {
		left = append(left, job.Name)
	}
	if !slices.Equal(left, []string{"j3", "j4", "j5"}) {
		t.Errorf("got jobs %v, want the 3 newest: j3 j4 j5", left)
	}
}

func TestDaemonSetRunsOnEveryReadyNode(t *testing.T) {
	st, c := newTestCluster(t)
	dc := &DaemonSetController{Client: c}

	st.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-1"}, NodeStatus: api.NodeStatus{Ready: true}})
	st.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-2"}, NodeStatus: api.NodeStatus{Ready: true}})
	st.PutNode(api.Node{ObjectMeta: api.ObjectMeta{Name: "node-3"}, NodeStatus: api.NodeStatus{Ready: false}})
	st.DaemonSets.Create(api.DaemonSet{ObjectMeta: meta("agent"), DaemonSetSpec: api.DaemonSetSpec{Template: busybox}})

	dc.reconcileAll()
	dc.reconcileAll() // a second check changes nothing

	pods := st.ListPods(ns)
	if len(pods) != 2 {
		t.Fatalf("got %d pods, want 2: one per ready node", len(pods))
	}
	for _, pod := range pods {
		if pod.Name != "agent-"+pod.NodeName {
			t.Errorf("pod %s is bound to %q, want its own node", pod.Name, pod.NodeName)
		}
	}

	// node-3 becomes ready: it gets a pod too.
	st.SetNodeReady("node-3", true)
	dc.reconcileAll()
	if n := len(st.ListPods(ns)); n != 3 {
		t.Errorf("after node-3 became ready: got %d pods, want 3", n)
	}

	// A new template replaces the pods one at a time, each once the rest are ready.
	setPhase(st, api.PodRunning, "agent-node-1", "agent-node-2", "agent-node-3")
	ds, _ := st.DaemonSets.Get(ns, "agent")
	ds.Template.Containers = []api.Container{{Name: "main", Image: "busybox:2"}}
	st.DaemonSets.Update(ds)

	dc.reconcileAll()
	var staying, leaving []string
	for _, pod := range st.ListPods(ns) {
		if pod.Terminating() {
			leaving = append(leaving, pod.Name)
		} else {
			staying = append(staying, pod.Name)
		}
	}
	if len(staying) != 2 || len(leaving) != 1 {
		t.Errorf("first step of the update: got pods %v and terminating %v, want 2 and 1 (one replaced at a time)", staying, leaving)
	}

	// While it terminates, nothing more happens: its replacement needs its name.
	dc.reconcileAll()
	if n := len(st.ListPods(ns)); n != 3 {
		t.Errorf("while one pod terminates: got %d pods, want still 3", n)
	}
}

func TestStatefulSetCreatesPodsInOrder(t *testing.T) {
	st, c := newTestCluster(t)
	sc := &StatefulSetController{Client: c}

	st.StatefulSets.Create(api.StatefulSet{ObjectMeta: meta("db"), StatefulSetSpec: api.StatefulSetSpec{Replicas: 3, Template: busybox}})

	// One at a time, each only once the one before it is ready.
	sc.reconcileAll()
	sc.reconcileAll()
	if names := podNames(st, "StatefulSet", "db"); !slices.Equal(names, []string{"db-0"}) {
		t.Fatalf("got %v, want only db-0 while it isn't ready", names)
	}
	for _, next := range []string{"db-1", "db-2"} {
		setPhase(st, api.PodRunning, podNames(st, "StatefulSet", "db")...)
		sc.reconcileAll()
		if names := podNames(st, "StatefulSet", "db"); !slices.Contains(names, next) {
			t.Fatalf("got %v, want %s next", names, next)
		}
	}
	setPhase(st, api.PodRunning, "db-2")

	// A deleted pod comes back with the same name.
	st.DeletePod(ns, "db-1")
	sc.reconcileAll()
	if _, ok := st.GetPod(ns, "db-1"); !ok {
		t.Error("db-1 was not created again")
	}
	setPhase(st, api.PodRunning, "db-1")

	// Scaling down removes the highest numbers first, one at a time.
	ss, _ := st.StatefulSets.Get(ns, "db")
	ss.Replicas = 1
	st.StatefulSets.Update(ss)
	sc.reconcileAll()
	if names := podNames(st, "StatefulSet", "db"); !slices.Equal(names, []string{"db-0", "db-1"}) {
		t.Errorf("first step of scaling down: got %v, want db-0 db-1", names)
	}
	sc.reconcileAll()
	if names := podNames(st, "StatefulSet", "db"); !slices.Equal(names, []string{"db-0"}) {
		t.Errorf("second step of scaling down: got %v, want db-0", names)
	}
}
