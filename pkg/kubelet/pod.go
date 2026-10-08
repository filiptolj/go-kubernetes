package kubelet

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/filiptolj/go-kubernetes/pkg/api"
	"github.com/filiptolj/go-kubernetes/pkg/cri"
)

// runningPod is a pod this kubelet runs.
type runningPod struct {
	pod    api.Pod
	cancel context.CancelFunc // stops the pod's container loops
	wg     sync.WaitGroup     // the container loops, so stopping can wait for them

	// The fields below are guarded by Kubelet.mu.
	stopping   bool                       // the kubelet is stopping it on purpose
	containers map[string]*containerState // by container name
	restarts   int
	reason     string       // such as "CrashLoopBackOff"
	phase      api.PodPhase // "" while running; Succeeded or Failed once finished

	// Status reports go out one at a time, so they can't arrive in the wrong
	// order. reported is the last one sent, so unchanged ones aren't resent.
	reportMu     sync.Mutex
	reported     api.PodStatus
	reportedOnce bool
}

// containerState is how one container of a pod is doing.
type containerState struct {
	up      bool   // running right now
	ready   bool   // passing its readiness check
	address string // where its port is reachable, while it runs
	done    bool   // exited for good: the restart policy says not to restart it
	failed  bool   // it exited with an error, when done
}

func newRunningPod(pod api.Pod, cancel context.CancelFunc) *runningPod {
	rp := &runningPod{pod: pod, cancel: cancel, containers: make(map[string]*containerState)}
	for _, c := range pod.Containers {
		rp.containers[c.Name] = &containerState{}
	}
	return rp
}

// startPod prepares a pod's volumes and environment, then starts a loop for
// each container, and one that keeps the pod's readiness up to date.
func (k *Kubelet) startPod(ctx context.Context, rp *runningPod) {
	pod := rp.pod
	log.Printf("starting pod %s", api.Key(pod.Namespace, pod.Name))

	// Logs belong to one pod. An older pod with the same name may have left
	// some behind; start empty. (Restarts of this pod's containers do add
	// to the same files, so the output of a crash stays readable.)
	err := os.RemoveAll(k.logDir(pod.Namespace, pod.Name))
	if err != nil {
		log.Printf("could not clear old logs: %v", err)
	}

	setups, err := k.prepare(pod)
	if err != nil {
		log.Printf("pod %s can't start: %v", api.Key(pod.Namespace, pod.Name), err)
		k.events.Warning("Pod", pod.Namespace, pod.Name, "FailedStart", "%v", err)
		k.finish(rp, api.PodFailed)
		return
	}

	// No report yet: the pod stays Pending until its first container has
	// started, and runOnce reports it Running.
	for _, c := range pod.Containers {
		rp.wg.Go(func() {
			k.runContainer(ctx, rp, c, setups[c.Name])
		})
	}
	go k.watchReadiness(ctx, rp)
}

// runContainer runs one container of a pod for as long as the pod lives.
// When the container exits, it is restarted if the pod's restart policy
// says so, after a delay that doubles while it keeps crashing. A container
// that fails its liveness probe is killed, which counts as a crash.
func (k *Kubelet) runContainer(ctx context.Context, rp *runningPod, c api.Container, setup containerSetup) {
	pod := rp.pod
	delay := k.cfg.RestartDelay

	for {
		started := time.Now()
		exitErr := k.runOnce(ctx, rp, c, setup)
		if ctx.Err() != nil {
			return // the pod is being stopped
		}

		if !shouldRestart(pod.RestartPolicy, exitErr) {
			k.containerDone(rp, c, exitErr)
			return
		}

		// It ran fine for a while, so it isn't crash-looping: start over with the shortest delay.
		if time.Since(started) >= k.cfg.HealthyAfter {
			delay = k.cfg.RestartDelay
		}

		k.events.Warning("Pod", pod.Namespace, pod.Name, "BackOff",
			"container %q %s; restarting it in %s", c.Name, describeExit(exitErr), delay)
		k.mu.Lock()
		rp.reason = "CrashLoopBackOff"
		k.mu.Unlock()
		k.report(rp)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		delay = min(delay*2, k.cfg.MaxRestartDelay)

		k.mu.Lock()
		rp.restarts++
		rp.reason = ""
		k.mu.Unlock()
	}
}

// runOnce starts a container and waits until it exits. It returns how it
// exited: nil for success.
func (k *Kubelet) runOnce(ctx context.Context, rp *runningPod, c api.Container, setup containerSetup) error {
	pod := rp.pod

	logs, err := k.openLog(pod, c)
	if err != nil {
		return err
	}
	defer logs.Close()

	running, err := k.runtime.Start(pod, c, cri.Options{Logs: logs, Env: setup.env, Mounts: setup.mounts})
	if err != nil {
		k.events.Warning("Pod", pod.Namespace, pod.Name, "FailedStart", "%v", err)
		return err
	}

	// If the pod was stopped while the container was starting, the stop may
	// have come too early to catch it. Stop it again now.
	if ctx.Err() != nil {
		k.runtime.Stop(pod, c)
		<-running.Done
		return nil
	}

	k.mu.Lock()
	state := rp.containers[c.Name]
	state.up, state.address = true, running.Address
	restarts := rp.restarts
	k.mu.Unlock()

	if restarts == 0 {
		k.events.Normal("Pod", pod.Namespace, pod.Name, "Started", "started container %q from image %q", c.Name, c.Image)
	} else {
		k.events.Normal("Pod", pod.Namespace, pod.Name, "Started", "restarted container %q", c.Name)
	}
	k.report(rp)

	exitErr := k.waitContainer(rp, c, running, time.Now())

	k.mu.Lock()
	state.up, state.ready = false, false
	k.mu.Unlock()
	return exitErr
}

// waitContainer waits for a running container to exit. Meanwhile it runs the
// container's liveness probe, if it has one, and kills the container when the
// probe fails too many times in a row.
func (k *Kubelet) waitContainer(rp *runningPod, c api.Container, running cri.Running, started time.Time) error {
	if c.LivenessProbe == nil || running.Address == "" {
		return <-running.Done
	}

	delay, period, threshold := probeSettings(c.LivenessProbe)
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	failures := 0
	for {
		select {
		case err := <-running.Done:
			return err
		case <-ticker.C:
		}

		if time.Since(started) < delay {
			continue
		}
		if check(c.LivenessProbe, running.Address) {
			failures = 0
			continue
		}

		failures++
		k.events.Warning("Pod", rp.pod.Namespace, rp.pod.Name, "Unhealthy",
			"liveness probe of container %q failed (%d of %d)", c.Name, failures, threshold)
		if failures < threshold {
			continue
		}

		k.events.Normal("Pod", rp.pod.Namespace, rp.pod.Name, "Killing",
			"container %q failed its liveness probe; killing it", c.Name)
		k.runtime.Stop(rp.pod, c)
		<-running.Done
		return errors.New("killed after failing its liveness probe")
	}
}

// shouldRestart applies a restart policy to a container that exited with exitErr.
func shouldRestart(policy api.RestartPolicy, exitErr error) bool {
	switch policy {
	case api.RestartNever:
		return false
	case api.RestartOnFailure:
		return exitErr != nil
	default: // Always, or "" for pods saved before restart policies existed
		return true
	}
}

// describeExit says how a container exited, for events.
func describeExit(err error) string {
	if err == nil {
		return "exited successfully"
	}
	return "exited: " + err.Error()
}

// containerDone records that a container won't be restarted. Once every
// container of the pod is done, the pod is finished.
func (k *Kubelet) containerDone(rp *runningPod, c api.Container, exitErr error) {
	pod := rp.pod
	if exitErr != nil {
		k.events.Warning("Pod", pod.Namespace, pod.Name, "Failed", "container %q exited: %v", c.Name, exitErr)
	}

	k.mu.Lock()
	state := rp.containers[c.Name]
	state.done, state.failed = true, exitErr != nil

	allDone, anyFailed := true, false
	for _, s := range rp.containers {
		allDone = allDone && s.done
		anyFailed = anyFailed || s.failed
	}
	stopping := rp.stopping
	k.mu.Unlock()

	if !allDone || stopping {
		k.report(rp)
		return
	}

	phase := api.PodSucceeded
	if anyFailed {
		phase = api.PodFailed
	} else {
		k.events.Normal("Pod", pod.Namespace, pod.Name, "Completed", "all containers exited successfully")
	}
	log.Printf("pod %s finished: %s", api.Key(pod.Namespace, pod.Name), phase)
	k.finish(rp, phase)
}

// finish records that a pod has finished, reports its final phase, and
// forgets it: it won't be started again.
func (k *Kubelet) finish(rp *runningPod, phase api.PodPhase) {
	key := api.Key(rp.pod.Namespace, rp.pod.Name)

	k.mu.Lock()
	rp.phase = phase
	if k.pods[key] == rp {
		delete(k.pods, key)
	}
	k.mu.Unlock()

	rp.cancel() // stops the readiness loop
	k.report(rp)
	k.removeVolumes(rp.pod)
}

// watchReadiness checks every container's readiness, and reports the pod's
// status when it changes, until the pod stops.
func (k *Kubelet) watchReadiness(ctx context.Context, rp *runningPod) {
	ticker := time.NewTicker(probeEvery)
	defer ticker.Stop()

	lastCheck := make(map[string]time.Time) // by container name
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		for _, c := range rp.pod.Containers {
			k.mu.Lock()
			state := rp.containers[c.Name]
			up, address := state.up, state.address
			k.mu.Unlock()

			// Containers without a readiness probe are checked on every tick;
			// those with one, as often as it says.
			period := probeEvery
			if c.ReadinessProbe != nil {
				_, period, _ = probeSettings(c.ReadinessProbe)
			}
			if up && time.Since(lastCheck[c.Name]) < period {
				continue
			}
			lastCheck[c.Name] = time.Now()

			ready := up && (c.Port == 0 || check(c.ReadinessProbe, address))
			k.mu.Lock()
			state.ready = ready
			k.mu.Unlock()
		}
		k.report(rp)
	}
}

// status works out what the pod's status should say. The caller must hold k.mu.
func (rp *runningPod) status() api.PodStatus {
	if rp.phase != "" {
		return api.PodStatus{Phase: rp.phase, Restarts: rp.restarts}
	}

	status := api.PodStatus{Phase: api.PodRunning, Ready: true, Restarts: rp.restarts, Reason: rp.reason}
	for _, c := range rp.pod.Containers {
		state := rp.containers[c.Name]
		status.Ready = status.Ready && state.ready
		if status.Address == "" && c.Port != 0 {
			status.Address = state.address
		}
	}
	return status
}

// report sends the pod's status to the API server, if it changed since the
// last report. A failed report is tried again on the next readiness check.
func (k *Kubelet) report(rp *runningPod) {
	rp.reportMu.Lock()
	defer rp.reportMu.Unlock()

	k.mu.Lock()
	status := rp.status()
	stopping := rp.stopping
	k.mu.Unlock()

	if stopping || (rp.reportedOnce && status == rp.reported) {
		return
	}

	err := k.client.SetPodStatus(rp.pod.Namespace, rp.pod.Name, status)
	if err != nil {
		log.Printf("could not report pod %s: %v", api.Key(rp.pod.Namespace, rp.pod.Name), err)
		return
	}

	if status.Ready && !rp.reported.Ready {
		k.events.Normal("Pod", rp.pod.Namespace, rp.pod.Name, "Ready", "all containers are ready")
	}
	rp.reported, rp.reportedOnce = status, true
}

// logDir returns the folder for a pod's container logs.
func (k *Kubelet) logDir(namespace, pod string) string {
	return filepath.Join(k.cfg.LogDir, namespace, pod)
}

// removeLogs deletes the logs of a deleted pod, unless a newer pod with the
// same name already runs here and writes there.
func (k *Kubelet) removeLogs(pod api.Pod) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, running := k.pods[api.Key(pod.Namespace, pod.Name)]; running {
		return
	}
	err := os.RemoveAll(k.logDir(pod.Namespace, pod.Name))
	if err != nil {
		log.Printf("could not remove logs: %v", err)
	}
}

// openLog opens a container's log file for appending, so the output of
// every restart ends up in the same file.
func (k *Kubelet) openLog(pod api.Pod, c api.Container) (*os.File, error) {
	dir := k.logDir(pod.Namespace, pod.Name)
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(filepath.Join(dir, c.Name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}
